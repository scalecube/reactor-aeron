package reactor.aeron;

import io.scalecube.trace.TraceReporter;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

/** Tracker and reporter of throughput rates. */
public class RateReporter implements Runnable, Disposable {

  private final long reportIntervalNs;
  private final Reporter reporter;
  private final Disposable disposable;

  private final LongAdder totalBytes = new LongAdder();
  private final LongAdder totalMessages = new LongAdder();

  private long lastTotalBytes;
  private long lastTotalMessages;
  private long lastTimestamp;

  private static final TraceReporter traceReporter = new TraceReporter();

  public RateReporter() {
    this(RateReporter::printRate);
  }

  /**
   * Create rate reporter.
   *
   * @param reporter reporter function
   */
  public RateReporter(Reporter reporter) {
    long reportDelayNs = Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY).toNanos();
    this.reportIntervalNs = Duration.ofSeconds(Configurations.REPORT_INTERVAL).toNanos();
    this.reporter = reporter;
    disposable =
        Schedulers.single()
            .schedulePeriodically(this, reportDelayNs, reportIntervalNs, TimeUnit.NANOSECONDS);
    
    if (traceReporter.isActive()) {
      traceReporter.scheduleDumpTo(Duration.ofSeconds(30), traceReporter.tracesEnv()+"throughput/");
    } 
    
  }

  @Override
  public void run() {
    long currentTotalMessages = totalMessages.longValue();
    long currentTotalBytes = totalBytes.longValue();
    long currentTimestamp = System.nanoTime();

    long timeSpanNs = currentTimestamp - lastTimestamp;
    double messagesPerSec =
        ((currentTotalMessages - lastTotalMessages) * (double) reportIntervalNs)
            / (double) timeSpanNs;
    final double bytesPerSec =
        ((currentTotalBytes - lastTotalBytes) * (double) reportIntervalNs) / (double) timeSpanNs;

    reporter.onReport(messagesPerSec, bytesPerSec, currentTotalMessages, currentTotalBytes);

    lastTotalBytes = currentTotalBytes;
    lastTotalMessages = currentTotalMessages;
    lastTimestamp = currentTimestamp;

    if (traceReporter.isActive()) {
      traceReporter.addY("reactor-aeron-throughput", messagesPerSec);
    }
  }

  @Override
  public void dispose() {
    disposable.dispose();
  }

  @Override
  public boolean isDisposed() {
    return disposable.isDisposed();
  }

  /**
   * Notify rate reporter of number of messages and bytes received, sent, etc.
   *
   * @param messages received, sent, etc.
   * @param bytes received, sent, etc.
   */
  public void onMessage(final long messages, final long bytes) {
    totalBytes.add(bytes);
    totalMessages.add(messages);
  }

  private static void printRate(
      final double messagesPerSec,
      final double bytesPerSec,
      final long totalFragments,
      final long totalBytes) {

    
      System.out.format(
          "%.07g msgs/sec, %.07g MB/sec, totals %d messages %d MB payloads%n",
          messagesPerSec, bytesPerSec / (1024 * 1024), totalFragments, totalBytes / (1024 * 1024));
    
  }

  /** Interface for reporting of rate information. */
  @FunctionalInterface
  public interface Reporter {
    /**
     * Called for a rate report.
     *
     * @param messagesPerSec since last report
     * @param bytesPerSec since last report
     * @param totalMessages since beginning of reporting
     * @param totalBytes since beginning of reporting
     */
    void onReport(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes);
  }
}

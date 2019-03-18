package reactor.aeron;

import io.scalecube.trace.TraceReporter;
import java.time.Duration;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class LatencyReporter {

  private static final TraceReporter reporter = new TraceReporter();

  private final Recorder histogram;

  private final String targetFolder =
      System.getProperty("reactor.aeron.report.folder.latency", "./target/traces/reports/latency/");

  private String name;

  public LatencyReporter(Recorder histogram, String name) {
    this.histogram = histogram;
    this.name = name;
  }

  public Disposable start() {
    return Disposables.composite(startReport(), startCollect());
  }

  
  private Disposable startCollect() {
    return Flux.interval(
            Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
            Duration.ofSeconds(Configurations.REPORT_INTERVAL))
        .publishOn(Schedulers.single())
        .doOnNext(i -> this.collect())
        .subscribe();
  }

  private Disposable startReport() {
    return startReport(targetFolder);
  }

  private Disposable startReport(String folder) {
    return Flux.interval(
            Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
            Duration.ofSeconds(Configurations.REPORT_INTERVAL + 30))
        .publishOn(Schedulers.single())
        .doOnNext(i -> this.report(folder))
        .subscribe();
  }

  private void report(String folder) {
    if (reporter.isActive()) {
      reporter
          .sendToJsonbin()
          .subscribe(
              res -> {
                if (res.success()) {
                  reporter.dumpToFile(folder, res.name(), res).subscribe();
                }
              });
    } else {
      System.out.println("---- PING/PONG HISTO ----");
      histogram.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
      System.out.println("---- PING/PONG HISTO ----");
    }
  }

  private void collect() {
    Histogram h = histogram.getIntervalHistogram();

    if (reporter.isActive()) {
      reporter.addY(this.name, h.getMean() / 1000.0);
    } else {
      System.out.println("---- PING/PONG HISTO ----");
      h.outputPercentileDistribution(System.out, 5, 1000.0, false);
      System.out.println("---- PING/PONG HISTO ----");
    }
  }
}

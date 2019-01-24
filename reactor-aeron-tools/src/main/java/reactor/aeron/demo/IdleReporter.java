package reactor.aeron.demo;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

/** Tracker and reporter of throughput rates. */
public class IdleReporter implements Runnable, Disposable {

  private final long reportIntervalNs;
  private final Disposable disposable;

  private final LongAdder zeros = new LongAdder();
  private final LongAdder spins = new LongAdder();
  private final LongAdder yields = new LongAdder();
  private final LongAdder parks = new LongAdder();

  private long lastTotalZeros;
  private long lastTotalSpins;
  private long lastTotalYields;
  private long lastTotalParks;
  private long lastTimestamp;

  /**
   * Create rate reporter.
   *
   * @param reportInterval reporter interval
   */
  public IdleReporter(Duration reportInterval) {
    this.reportIntervalNs = reportInterval.toNanos();
    disposable =
        Schedulers.single()
            .schedulePeriodically(this, reportIntervalNs, reportIntervalNs, TimeUnit.NANOSECONDS);
  }

  @Override
  public void run() {
    long currentTotalZeros = zeros.longValue();
    long currentTotalSpins = spins.longValue();
    long currentTotalYields = yields.longValue();
    long currentTotalParks = parks.longValue();
    long currentTimestamp = System.nanoTime();

    long timeSpanNs = currentTimestamp - lastTimestamp;
    double zerosPerSec =
        ((currentTotalZeros - lastTotalZeros) * (double) reportIntervalNs) / (double) timeSpanNs;
    double spinsCountPerSec =
        ((currentTotalSpins - lastTotalSpins) * (double) reportIntervalNs) / (double) timeSpanNs;
    double yieldsCountPerSec =
        ((currentTotalYields - lastTotalYields) * (double) reportIntervalNs) / (double) timeSpanNs;
    double parksCountPerSec =
        ((currentTotalParks - lastTotalParks) * (double) reportIntervalNs) / (double) timeSpanNs;

    System.out.format(
        "%.10g zero/sec, %.10g spins/sec, %.10g yields/sec, %.10g parks/sec, totals %d zero %d spins %d yields %d parks%n",
        zerosPerSec,
        spinsCountPerSec,
        yieldsCountPerSec,
        parksCountPerSec,
        currentTotalZeros,
        currentTotalSpins,
        currentTotalYields,
        currentTotalParks);

    lastTotalZeros = currentTotalZeros;
    lastTotalSpins = currentTotalSpins;
    lastTotalYields = currentTotalYields;
    lastTotalParks = currentTotalParks;
    lastTimestamp = currentTimestamp;
  }

  @Override
  public void dispose() {
    disposable.dispose();
  }

  @Override
  public boolean isDisposed() {
    return disposable.isDisposed();
  }

  public void incZeros() {
    zeros.increment();
  }

  public void incSpins() {
    spins.increment();
  }

  public void incYields() {
    yields.increment();
  }

  public void incParks() {
    parks.increment();
  }
}

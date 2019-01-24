package reactor.aeron.demo;

import java.util.concurrent.locks.LockSupport;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.hints.ThreadHints;

abstract class BackoffIdleStrategyPrePad {
  @SuppressWarnings("unused")
  long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
}

@SuppressWarnings("WeakerAccess")
abstract class BackoffIdleStrategyData extends BackoffIdleStrategyPrePad {
  protected static final int NOT_IDLE = 0;
  protected static final int SPINNING = 1;
  protected static final int YIELDING = 2;
  protected static final int PARKING = 3;

  protected final long maxSpins;
  protected final long maxYields;
  protected final long minParkPeriodNs;
  protected final long maxParkPeriodNs;

  protected int state = NOT_IDLE;
  protected long spins;
  protected long yields;
  protected long parkPeriodNs;

  BackoffIdleStrategyData(
      final long maxSpins,
      final long maxYields,
      final long minParkPeriodNs,
      final long maxParkPeriodNs) {
    this.maxSpins = maxSpins;
    this.maxYields = maxYields;
    this.minParkPeriodNs = minParkPeriodNs;
    this.maxParkPeriodNs = maxParkPeriodNs;
  }
}

public class BackoffIdleStrategyWithReporter extends BackoffIdleStrategyData
    implements IdleStrategy {

  @SuppressWarnings("unused")
  long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;

  private final IdleReporter reporter;

  /**
   * Create a set of state tracking idle behavior
   *
   * @param maxSpins to perform before moving to {@link Thread#yield()}
   * @param maxYields to perform before moving to {@link
   *     java.util.concurrent.locks.LockSupport#parkNanos(long)}
   * @param minParkPeriodNs to use when initiating parking
   * @param maxParkPeriodNs to use when parking
   */
  public BackoffIdleStrategyWithReporter(
      final long maxSpins,
      final long maxYields,
      final long minParkPeriodNs,
      final long maxParkPeriodNs,
      IdleReporter reporter) {
    super(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs);
    this.reporter = reporter;
  }

  /** {@inheritDoc} */
  public void idle(final int workCount) {
    if (workCount > 0) {
      reset();
    } else {
      idle();
    }
  }

  public void idle() {
    switch (state) {
      case NOT_IDLE:
        state = SPINNING;
        spins++;
        reporter.incSpins();
        break;

      case SPINNING:
        ThreadHints.onSpinWait();
        if (++spins > maxSpins) {
          state = YIELDING;
          yields = 0;
        }
        reporter.incSpins();
        break;

      case YIELDING:
        if (++yields > maxYields) {
          state = PARKING;
          parkPeriodNs = minParkPeriodNs;
        } else {
          Thread.yield();
        }
        reporter.incYields();
        break;

      case PARKING:
        LockSupport.parkNanos(parkPeriodNs);
        parkPeriodNs = Math.min(parkPeriodNs << 1, maxParkPeriodNs);
        reporter.incParks();
        break;
    }
  }

  public void reset() {
    spins = 0;
    yields = 0;
    parkPeriodNs = minParkPeriodNs;
    state = NOT_IDLE;
    reporter.incZeros();
  }
}

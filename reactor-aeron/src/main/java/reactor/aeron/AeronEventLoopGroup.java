package reactor.aeron;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;

/**
 * Wrapper around the {@link AeronEventLoop} where the actual logic is performed. Manages grouping
 * of multiple instances of {@link AeronEventLoop}: round-robin iteration and grouped disposal.
 */
public class AeronEventLoopGroup implements AutoCloseable {

  private final int id = System.identityHashCode(this);
  private final AeronEventLoop[] eventLoops;
  private final AtomicInteger idx = new AtomicInteger();

  /**
   * Constructor.
   *
   * @param name thread name
   * @param numOfWorkers number of {@link AeronEventLoop} instances in the group
   * @param workerIdleStrategySupplier factory for {@link IdleStrategy} instances
   */
  public AeronEventLoopGroup(
      String name, int numOfWorkers, Supplier<IdleStrategy> workerIdleStrategySupplier) {
    this.eventLoops = new AeronEventLoop[numOfWorkers];
    for (int i = 0; i < numOfWorkers; i++) {
      eventLoops[i] = new AeronEventLoop(name, i, id, workerIdleStrategySupplier.get());
    }
  }

  /**
   * Get instance of worker from the group (round-robin iteration).
   *
   * @return instance of worker in the group
   */
  public AeronEventLoop next() {
    return eventLoops[Math.abs(idx.getAndIncrement() % eventLoops.length)];
  }

  public AeronEventLoop first() {
    return eventLoops[0];
  }

  @Override
  public void close() {
    CloseHelper.quietCloseAll(eventLoops);
  }

  @Override
  public String toString() {
    return "AeronEventLoopGroup" + id;
  }
}

package reactor.aeron;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Wrapper around the {@link AeronEventLoop} where the actual logic is performed. Manages grouping
 * of multiple instances of {@link AeronEventLoop}: round-robin iteration and grouped disposal.
 */
class AeronEventLoopGroup implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronEventLoopGroup.class);

  private final int id = System.identityHashCode(this);
  private final AeronEventLoop[] eventLoops;
  private final AtomicInteger idx = new AtomicInteger();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param name thread name
   * @param numOfWorkers number of {@link AeronEventLoop} instances in the group
   * @param workerIdleStrategySupplier factory for {@link IdleStrategy} instances
   */
  AeronEventLoopGroup(
      String name, int numOfWorkers, Supplier<IdleStrategy> workerIdleStrategySupplier) {
    this.eventLoops = new AeronEventLoop[numOfWorkers];
    for (int i = 0; i < numOfWorkers; i++) {
      eventLoops[i] = new AeronEventLoop(name, i, id, workerIdleStrategySupplier.get());
    }

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  /**
   * Get instance of worker from the group (round-robin iteration).
   *
   * @return instance of worker in the group
   */
  AeronEventLoop next() {
    return eventLoops[Math.abs(idx.getAndIncrement() % eventLoops.length)];
  }

  AeronEventLoop first() {
    return eventLoops[0];
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  private Mono<Void> doDispose() {
    return Mono.defer(
        () -> {
          logger.debug("Disposing {}", this);
          return Mono.whenDelayError(
              Arrays.stream(eventLoops)
                  .peek(AeronEventLoop::dispose)
                  .map(AeronEventLoop::onDispose)
                  .toArray(Mono<?>[]::new));
        });
  }

  @Override
  public String toString() {
    return "AeronEventLoopGroup" + id;
  }
}

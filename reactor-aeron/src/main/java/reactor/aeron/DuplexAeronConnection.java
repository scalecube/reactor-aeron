package reactor.aeron;

import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron <i>connection</i>. Bound to certain {@code sessionId}. Implements {@link
 * OnDisposable} for convenient resource cleanup.
 */
final class DuplexAeronConnection<I> implements AeronConnection<I> {

  private final Logger logger = LoggerFactory.getLogger(DuplexAeronConnection.class);

  private final int sessionId;

  private final AeronInbound<I> inbound;
  private final AeronOutbound outbound;

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param sessionId session id
   * @param inbound inbound
   * @param outbound outbound
   * @param disposeHook shutdown hook
   */
  DuplexAeronConnection(
      int sessionId,
      AeronInbound<I> inbound,
      AeronOutbound outbound,
      MonoProcessor<Void> disposeHook) {

    this.sessionId = sessionId;
    this.inbound = inbound;
    this.outbound = outbound;

    dispose
        .or(disposeHook)
        .then(doDispose())
        .doFinally(s -> logger.debug("{}: connection disposed", Integer.toHexString(sessionId)))
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  /**
   * Setting up this connection by applying user provided application level handler.
   *
   * @param handler handler with application level code
   */
  Mono<AeronConnection<I>> start(
      Function<? super AeronConnection<I>, ? extends Publisher<Void>> handler) {
    return Mono.fromRunnable(() -> start0(handler)).thenReturn(this);
  }

  private void start0(Function<? super AeronConnection<I>, ? extends Publisher<Void>> handler) {
    if (handler == null) {
      logger.warn(
          "{}: connection handler function is not specified", Integer.toHexString(sessionId));
    } else if (!isDisposed()) {
      handler.apply(this).subscribe(disposeSubscriber());
    }
  }

  @Override
  public AeronInbound<I> inbound() {
    return inbound;
  }

  @Override
  public AeronOutbound outbound() {
    return outbound;
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
              Mono.fromRunnable(inbound::dispose), Mono.fromRunnable(outbound::dispose));
        });
  }

  @Override
  public String toString() {
    return "DefaultAeronConnection" + Integer.toHexString(sessionId);
  }
}

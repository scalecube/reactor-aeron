package reactor.aeron;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron <i>connection</i>. Bound to certain {@code sessionId}. Implements {@link
 * OnDisposable} for convenient resource cleanup.
 */
public final class DefaultAeronConnection implements Connection {

  private final Logger logger = LoggerFactory.getLogger(DefaultAeronConnection.class);

  private final int sessionId;

  private final AeronInbound inbound;
  private final AeronOutbound outbound;
  private final MessagePublication publication;
  private final MessageSubscription subscription;

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param sessionId session id
   * @param inbound inbound
   * @param outbound outbound
   * @param publication publication
   */
  public DefaultAeronConnection(
      int sessionId, AeronInbound inbound, AeronOutbound outbound, MessagePublication publication) {
    this(sessionId, inbound, outbound, null, publication);
  }

  /**
   * Constructor.
   *
   * @param sessionId session id
   * @param inbound inbound
   * @param outbound outbound
   * @param subscription subscription
   * @param publication publication
   */
  public DefaultAeronConnection(
      int sessionId,
      AeronInbound inbound,
      AeronOutbound outbound,
      MessageSubscription subscription,
      MessagePublication publication) {

    this.sessionId = sessionId;
    this.inbound = inbound;
    this.outbound = outbound;
    this.subscription = subscription;
    this.publication = publication;

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  @Override
  public AeronInbound inbound() {
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
              Mono.fromRunnable(((Disposable) inbound)::dispose),
              Optional.ofNullable(subscription)
                  .map(s -> Mono.fromRunnable(s::dispose).then(s.onDispose()))
                  .orElse(Mono.empty()),
              Mono.fromRunnable(publication::dispose).then(publication.onDispose()));
        });
  }

  @Override
  public String toString() {
    return "DefaultAeronConnection0x" + Integer.toHexString(sessionId);
  }
}

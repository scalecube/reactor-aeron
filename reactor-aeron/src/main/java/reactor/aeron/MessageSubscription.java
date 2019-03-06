package reactor.aeron;

import io.aeron.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

class MessageSubscription implements OnDisposable, AeronResource {

  private static final Logger logger = LoggerFactory.getLogger(MessageSubscription.class);

  private final AeronEventLoop eventLoop;
  private final Subscription subscription; // aeron subscription

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param subscription aeron subscription
   * @param eventLoop event loop where this {@code MessageSubscription} is assigned
   */
  MessageSubscription(Subscription subscription, AeronEventLoop eventLoop) {
    this.subscription = subscription;
    this.eventLoop = eventLoop;
  }

  @Override
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw AeronExceptions.failWithResourceDisposal("aeron subscription");
    }
    try {
      subscription.close();
      logger.debug("Disposed {}", this);
    } catch (Exception ex) {
      logger.warn("{} failed on aeron.Subscription close(): {}", this, ex.toString());
      throw Exceptions.propagate(ex);
    } finally {
      onDispose.onComplete();
    }
  }

  @Override
  public void dispose() {
    eventLoop
        .dispose(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  /**
   * Delegates to {@link Subscription#isClosed()}.
   *
   * @return {@code true} if aeron {@code Subscription} is closed, {@code false} otherwise
   */
  @Override
  public boolean isDisposed() {
    return subscription.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  @Override
  public String toString() {
    return "MessageSubscription{sub=" + subscription.channel() + "}";
  }
}

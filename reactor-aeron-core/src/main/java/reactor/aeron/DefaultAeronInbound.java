package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public final class DefaultAeronInbound implements AeronInbound, OnDisposable {

  private final String name;
  private final AeronResources resources;

  private volatile ByteBufferFlux flux;
  private volatile MessageSubscription subscription;

  public DefaultAeronInbound(String name, AeronResources resources) {
    this.name = name;
    this.resources = resources;
  }

  /**
   * Starts inbound.
   *
   * @param channel server or client channel uri
   * @param streamId stream id
   * @param sessionId session id
   * @param onCompleteHandler callback which will be invoked when this finishes
   * @return success result
   */
  public Mono<Void> start(
      String channel, int streamId, long sessionId, Runnable onCompleteHandler) {
    return Mono.defer(
        () -> {
          DataMessageProcessor messageProcessor =
              new DataMessageProcessor(name, sessionId, onCompleteHandler);

          flux = new ByteBufferFlux(messageProcessor);

          AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .dataSubscription(
                  name,
                  channel,
                  streamId,
                  messageProcessor,
                  eventLoop,
                  null,
                  image -> Optional.ofNullable(onCompleteHandler).ifPresent(Runnable::run))
              .doOnSuccess(
                  result -> {
                    subscription = result;
                    messageProcessor.onSubscription(subscription);
                  })
              .then()
              .log("inbound");
        });
  }

  @Override
  public ByteBufferFlux receive() {
    return flux;
  }

  @Override
  public void dispose() {
    if (subscription != null) {
      subscription.dispose();
    }
  }

  @Override
  public Mono<Void> onDispose() {
    return subscription != null ? subscription.onDispose() : Mono.empty();
  }

  @Override
  public boolean isDisposed() {
    return subscription != null && subscription.isDisposed();
  }

  private static class DataMessageProcessor
      implements DataMessageSubscriber, Publisher<ByteBuffer> {

    private static final Logger logger = LoggerFactory.getLogger(DataMessageProcessor.class);

    private final String category;
    private final long sessionId;
    private final Runnable onCompleteHandler;

    private volatile Subscription subscription;
    private volatile Subscriber<? super ByteBuffer> subscriber;

    private DataMessageProcessor(String category, long sessionId, Runnable onCompleteHandler) {
      this.category = category;
      this.sessionId = sessionId;
      this.onCompleteHandler = onCompleteHandler;
    }

    @Override
    public void onSubscription(Subscription subscription) {
      this.subscription = subscription;
    }

    @Override
    public void onNext(long sessionId, ByteBuffer buffer) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}, buffer: {}",
            category,
            MessageType.NEXT,
            sessionId,
            buffer);
      }

      if (this.sessionId == sessionId) {
        subscriber.onNext(buffer);
      } else {
        logger.error(
            "[{}] Received {} for unexpected sessionId: {}", category, MessageType.NEXT, sessionId);
      }
    }

    @Override
    public void onComplete(long sessionId) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "[{}] Received {} for sessionId: {}", category, MessageType.COMPLETE, sessionId);
      }

      if (this.sessionId == sessionId) {
        onCompleteHandler.run();
      } else {
        logger.error(
            "[{}] Received {} for unexpected sessionId: {}",
            category,
            MessageType.COMPLETE,
            sessionId);
      }
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
      this.subscriber = subscriber;
      subscriber.onSubscribe(subscription);
    }
  }
}

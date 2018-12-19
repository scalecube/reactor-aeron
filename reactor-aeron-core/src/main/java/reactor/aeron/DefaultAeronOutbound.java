package reactor.aeron;

import java.nio.ByteBuffer;
import java.time.Duration;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class DefaultAeronOutbound implements AeronOutbound, OnDisposable {

  private static final Logger logger = Loggers.getLogger(DefaultAeronOutbound.class);

  private static final RuntimeException NOT_CONNECTED_EXCEPTION =
      new RuntimeException("publication is not connected");

  private final String category;
  private final String channel;
  private final AeronResources resources;
  private final AeronOptions options;

  private volatile AeronWriteSequencer sequencer;
  private volatile MessagePublication publication;

  /**
   * Constructor.
   *
   * @param category category
   * @param channel channel
   * @param resources resources
   * @param options options
   */
  public DefaultAeronOutbound(
      String category, String channel, AeronResources resources, AeronOptions options) {
    this.category = category;
    this.channel = channel;
    this.resources = resources;
    this.options = options;
  }

  /**
   * Init method. Creates data publication and assigns it to event loop. Makes this object eligible
   * for calling {@link #send(Publisher)} function.
   *
   * @param sessionId session id
   * @param streamId stream id
   * @return initialization handle
   */
  public Mono<Void> start(long sessionId, int streamId) {
    return Mono.defer(
        () -> {
          final AeronEventLoop eventLoop = resources.nextEventLoop();

          return resources
              .messagePublication(category, channel, streamId, options, eventLoop)
              .doOnSuccess(
                  result -> {
                    publication = result;
                    sequencer = new AeronWriteSequencer(sessionId, publication, eventLoop);
                  })
              .flatMap(
                  result -> {
                    Duration retryInterval = Duration.ofMillis(100);
                    Duration connectTimeout = options.connectTimeout();
                    long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

                    return Mono.fromCallable(publication::isConnected)
                        .filter(isConnected -> isConnected)
                        .switchIfEmpty(Mono.error(NOT_CONNECTED_EXCEPTION))
                        .retryBackoff(retryCount, retryInterval, retryInterval)
                        .timeout(connectTimeout)
                        .then()
                        .onErrorResume(
                            th -> {
                              logger.warn(
                                  "Failed to connect publication {} for sending data during {}",
                                  publication,
                                  connectTimeout);
                              dispose();
                              return Mono.error(th);
                            });
                  })
              .log("defaultOutbound");
        });
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuffer> dataStream) {
    return then(new SendPublisher(sequencer.sessionId, dataStream, publication, sequencer.eventLoop)
        .then());
    // return then(sequencer.write(dataStream));
  }

  @Override
  public Mono<Void> then() {
    return Mono.empty();
  }

  @Override
  public void dispose() {
    // TODO think of effectively and safelly closing correct reference of sequencer
    if (sequencer != null) {
      sequencer.dispose();
    }
    if (publication != null) {
      publication.dispose();
    }
  }

  @Override
  public boolean isDisposed() {
    return publication != null && publication.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return publication != null ? publication.onDispose() : Mono.empty();
  }
}

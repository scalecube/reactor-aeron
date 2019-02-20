package reactor.aeron;

import io.aeron.Image;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class ReplayingClientConnector {

  private static final Logger logger = LoggerFactory.getLogger(ReplayingClientConnector.class);

  /** The stream ID that the server and client use for messages. */
  private static final int STREAM_ID = 0xcafe0000;

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super AeronConnection, ? extends Publisher<Void>> handler;

  ReplayingClientConnector(AeronOptions options) {
    this.options = options;
    this.resources = options.resources();
    this.handler = options.handler();
  }

  /**
   * Creates and setting up {@link AeronConnection} object and everyting around it.
   *
   * @return mono result
   */
  Mono<AeronConnection> start() {
    return Mono.defer(
        () -> {
          AeronEventLoop eventLoop = resources.nextEventLoop();

          return tryConnect(eventLoop)
              .flatMap(
                  publication -> {
                    // inbound->MDC(xor(sessionId))->Sub(control-endpoint, xor(sessionId))
                    int sessionId = publication.sessionId();
                    String inboundChannel =
                        options
                            .inboundUri()
                            .uri(b -> b.sessionId(sessionId ^ Integer.MAX_VALUE))
                            .asString();
                    logger.debug(
                        "{}: creating client connection: {}",
                        Integer.toHexString(sessionId),
                        inboundChannel);

                    // setup cleanup hook to use it onwards
                    MonoProcessor<Void> disposeHook = MonoProcessor.create();
                    // setup image avaiable hook
                    MonoProcessor<Image> inboundAvailable = MonoProcessor.create();

                    return resources
                        .subscription(
                            inboundChannel,
                            STREAM_ID,
                            eventLoop,
                            image -> {
                              logger.debug(
                                  "{}: created client inbound", Integer.toHexString(sessionId));
                              inboundAvailable.onNext(image);
                            },
                            image -> {
                              logger.debug(
                                  "{}: client inbound became unavaliable",
                                  Integer.toHexString(sessionId));
                              disposeHook.onComplete();
                            })
                        .doOnError(
                            th -> {
                              logger.warn(
                                  "{}: failed to create client inbound, cause: {}",
                                  Integer.toHexString(sessionId),
                                  th.toString());
                              // dispose outbound resource
                              publication.dispose();
                            })
                        .flatMap(
                            subscription ->
                                inboundAvailable.flatMap(
                                    image ->
                                        newConnection(
                                            sessionId,
                                            image,
                                            publication,
                                            subscription,
                                            disposeHook,
                                            eventLoop)))
                        .doOnSuccess(
                            connection ->
                                logger.debug(
                                    "{}: created client connection: {}",
                                    Integer.toHexString(sessionId),
                                    inboundChannel));
                  });
        });
  }

  private Mono<AeronConnection> newConnection(
      int sessionId,
      Image image,
      MessagePublication publication,
      MessageSubscription subscription,
      MonoProcessor<Void> disposeHook,
      AeronEventLoop eventLoop) {

    return resources
        .inbound(image, subscription, eventLoop)
        .doOnError(
            ex -> {
              subscription.dispose();
              publication.dispose();
            })
        .flatMap(
            inbound -> {
              DefaultAeronOutbound outbound = new DefaultAeronOutbound(publication);

              DuplexAeronConnection connection =
                  new DuplexAeronConnection(sessionId, inbound, outbound, disposeHook, resources.aeronArchive());

              return connection.start(handler).doOnError(ex -> connection.dispose());
            });
  }

  private Mono<MessagePublication> tryConnect(AeronEventLoop eventLoop) {
    return Mono.defer(
        () -> {
          int retryCount = options.connectRetryCount();
          Duration retryInterval = options.connectTimeout();

          // outbound->Pub(endpoint, sessionId)
          return Mono.fromCallable(this::getOutboundChannel)
              .flatMap(channel -> getPublication(eventLoop, channel))
              .flatMap(mp -> mp.ensureConnected().doOnError(ex -> mp.dispose()))
              .retryBackoff(retryCount, Duration.ZERO, retryInterval)
              .doOnError(
                  ex -> logger.warn("aeron.Publication is not connected after several retries"));
        });
  }

  private Mono<MessagePublication> getPublication(AeronEventLoop eventLoop, String channel) {
    return resources.publication(channel, STREAM_ID, options, eventLoop);
  }

  private String getOutboundChannel() {
    AeronChannelUriString outboundUri = options.outboundUri();
    Supplier<Integer> sessionIdGenerator = options.sessionIdGenerator();

    return sessionIdGenerator != null && outboundUri.builder().sessionId() == null
        ? outboundUri.uri(opts -> opts.sessionId(sessionIdGenerator.get())).asString()
        : outboundUri.asString();
  }
}

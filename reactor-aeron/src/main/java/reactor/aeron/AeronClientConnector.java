package reactor.aeron;

import io.aeron.Image;
import io.aeron.Publication;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron client connector. Schematically can be described as:
 *
 * <pre>
 * Client
 * serverPort->outbound->Pub(endpoint, sessionId)
 * serverControlPort->inbound->MDC(xor(sessionId))->Sub(control-endpoint, xor(sessionId))</pre>
 */
final class AeronClientConnector {

  private static final Logger logger = LoggerFactory.getLogger(AeronClientConnector.class);

  /** The stream ID that the server and client use for messages. */
  private static final int STREAM_ID = 0xcafe0000;

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super AeronConnection, ? extends Publisher<Void>> handler;
  private final DefaultFragmentMapper mapper = new DefaultFragmentMapper();

  AeronClientConnector(AeronOptions options) {
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
          return tryConnect()
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
                              CloseHelper.quietClose(publication);
                            })
                        .flatMap(
                            subscription ->
                                inboundAvailable.flatMap(
                                    image ->
                                        newConnection(sessionId, image, publication, disposeHook)))
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
      int sessionId, Image image, Publication publication, MonoProcessor<Void> disposeHook) {
    PublicationAgent publicationAgent = new PublicationAgent(publication);
    ImageAgent<DirectBuffer> imageAgent = new ImageAgent<>(image, mapper, true);
    DuplexAeronConnection connection =
        new DuplexAeronConnection(sessionId, imageAgent, publicationAgent, disposeHook);
    return connection
        .start(handler)
        .doOnSuccess(
            c -> {
              AeronEventLoop eventLoop = resources.nextEventLoop();
              eventLoop.register(imageAgent);
              eventLoop.register(publicationAgent);
            })
        .doOnError(ex -> connection.dispose());
  }

  private Mono<Publication> tryConnect() {
    return Mono.defer(
        () -> {
          int retryCount = options.connectRetryCount();
          Duration retryInterval = options.connectTimeout();

          // outbound->Pub(endpoint, sessionId)
          return Mono.fromCallable(this::getOutboundChannel)
              .flatMap(channel -> resources.publication(channel, STREAM_ID))
              .flatMap(
                  publication ->
                      ensureConnected(publication)
                          .doOnError(ex -> CloseHelper.quietClose(publication)))
              .retryBackoff(retryCount, Duration.ZERO, retryInterval)
              .doOnError(
                  ex -> logger.warn("aeron.Publication is not connected after several retries"));
        });
  }

  /**
   * Spins (in async fashion) until {@link Publication#isConnected()} would have returned {@code
   * true} or {@code connectTimeout} elapsed.
   *
   * @return mono result
   */
  private Mono<Publication> ensureConnected(Publication publication) {
    return Mono.defer(
        () -> {
          Duration connectTimeout = options.connectTimeout();
          Duration retryInterval = Duration.ofMillis(100);
          long retryCount = Math.max(connectTimeout.toMillis() / retryInterval.toMillis(), 1);

          return ensureConnected0(publication)
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .doOnError(
                  ex -> logger.warn("aeron.Publication is not connected after several retries"))
              .thenReturn(publication);
        });
  }

  private Mono<Void> ensureConnected0(Publication publication) {
    return Mono.defer(
        () ->
            publication.isConnected()
                ? Mono.empty()
                : Mono.error(
                    AeronExceptions.failWithPublication("aeron.Publication is not connected")));
  }

  private String getOutboundChannel() {
    AeronChannelUriString outboundUri = options.outboundUri();
    Supplier<Integer> sessionIdGenerator = options.sessionIdGenerator();

    return sessionIdGenerator != null && outboundUri.builder().sessionId() == null
        ? outboundUri.uri(opts -> opts.sessionId(sessionIdGenerator.get())).asString()
        : outboundUri.asString();
  }
}

package reactor.aeron.mdc;

import io.aeron.Image;
import io.aeron.Subscription;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronDuplex;
import reactor.aeron.AeronEventLoop;
import reactor.aeron.DefaultAeronDuplex;
import reactor.aeron.DefaultFragmentMapper;
import reactor.aeron.ImageAgent;
import reactor.aeron.OnDisposable;
import reactor.aeron.PublicationAgent;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Full-duplex aeron server handler. Schematically can be described as:
 *
 * <pre>
 * Server
 * serverPort->inbound->Sub(endpoint, acceptor[onImageAvailable, onImageUnavailbe])
 * + onImageAvailable(Image)
 * sessionId->inbound->EmitterPocessor
 * serverControlPort->outbound->MDC(xor(sessionId))->Pub(control-endpoint, xor(sessionId))
 * </pre>
 */
final class AeronServerHandler implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  /** The stream ID that the server and client use for messages. */
  private static final int STREAM_ID = 0xcafe0000;

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super AeronDuplex<DirectBuffer>, ? extends Publisher<Void>>
      handler;
  private final DefaultFragmentMapper mapper = new DefaultFragmentMapper();

  private volatile Subscription acceptorSubscription; // server acceptor subscription

  private final Map<Integer, OnDisposable> connections = new ConcurrentHashMap<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronServerHandler(AeronOptions options) {
    this.options = options;
    this.resources = options.resources();
    this.handler = options.handler();

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  Mono<OnDisposable> start() {
    return Mono.defer(
        () -> {
          // Sub(endpoint{address:serverPort})
          String acceptorChannel = options.inboundUri().asString();

          logger.debug("Starting {} on: {}", this, acceptorChannel);
          return resources
              .subscription(
                  acceptorChannel, STREAM_ID, this::onImageAvailable, this::onImageUnavailable)
              .doOnSuccess(s -> this.acceptorSubscription = s)
              .thenReturn(this)
              .doOnSuccess(handler -> logger.debug("Started {} on: {}", this, acceptorChannel))
              .doOnError(
                  ex -> {
                    logger.error("Failed to start {} on: {}", this, acceptorChannel);
                    dispose();
                  });
        });
  }

  /**
   * Setting up new {@link AeronDuplex} identified by {@link Image#sessionId()}.
   * Specifically creates Multi Destination Cast (MDC) message publication (aeron {@link
   * io.aeron.Publication} underneath) with control-endpoint, control-mode and XOR-ed image
   * sessionId. Essentially creates <i>server-side-individual-MDC</i>.
   *
   * @param image source image
   */
  private void onImageAvailable(Image image) {
    // Pub(control-endpoint{address:serverControlPort}, xor(sessionId))->MDC(xor(sessionId))
    int sessionId = image.sessionId();
    String outboundChannel =
        options.outboundUri().uri(b -> b.sessionId(sessionId ^ Integer.MAX_VALUE)).asString();

    logger.debug(
        "{}: creating server connection: {}", Integer.toHexString(sessionId), outboundChannel);

    resources
        .publication(outboundChannel, STREAM_ID)
        .map(
            publication -> {
              PublicationAgent outbound = new PublicationAgent(publication);
              ImageAgent<DirectBuffer> inbound = new ImageAgent<>(image, mapper, false);
              return new DefaultAeronDuplex<>(inbound, outbound);
            })
        .doOnSuccess(
            connection -> {
              if (handler == null) {
                logger.warn(
                    "{}: connection handler function is not specified",
                    Integer.toHexString(sessionId));
              } else if (!connection.isDisposed()) {
                handler.apply(connection).subscribe(connection.disposeSubscriber());
              }

              AeronEventLoop eventLoop = resources.nextEventLoop();
              eventLoop.register((Agent) connection.inbound());
              eventLoop.register((Agent) connection.outbound());
              connections.put(sessionId, connection);
              connection.onDispose(() -> connections.remove(sessionId));
            })
        .doOnSuccess(
            connection ->
                logger.debug(
                    "{}: created server connection: {}",
                    Integer.toHexString(sessionId),
                    outboundChannel))
        .subscribe(
            null,
            ex ->
                logger.warn(
                    "{}: failed to create server outbound, cause: {}",
                    Integer.toHexString(sessionId),
                    ex.toString()));
  }

  /**
   * Disposes {@link AeronDuplex} corresponding to {@link Image#sessionId()}.
   *
   * @param image source image
   */
  private void onImageUnavailable(Image image) {
    logger.debug("{}: server inbound became unavailable", Integer.toHexString(image.sessionId()));
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
    return Mono.fromRunnable(
        () -> {
          logger.debug("Disposing {}", this);
          // dispose server acceptor subscription
          CloseHelper.quietClose(acceptorSubscription);
          // dispose all existing connections
          connections.forEach((sessionId, connection) -> connection.dispose());
        });
  }

  @Override
  public String toString() {
    return "AeronServerHandler" + Integer.toHexString(System.identityHashCode(this));
  }
}

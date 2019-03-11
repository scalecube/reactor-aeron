package reactor.aeron;

import io.aeron.Image;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final Function<? super AeronConnection, ? extends Publisher<Void>> handler;

  private volatile MessageSubscription acceptorSubscription; // server acceptor subscription

  private final Map<Integer, MonoProcessor<Void>> disposeHooks = new ConcurrentHashMap<>();

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
                  acceptorChannel,
                  STREAM_ID,
                  resources.firstEventLoop(),
                  this::onImageAvailable,
                  this::onImageUnavailable)
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
   * Setting up new {@link AeronConnection} identified by {@link Image#sessionId()}. Specifically
   * creates Multi Destination Cast (MDC) message publication (aeron {@link io.aeron.Publication}
   * underneath) with control-endpoint, control-mode and XOR-ed image sessionId. Essentially creates
   * <i>server-side-individual-MDC</i>.
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

    AeronEventLoop eventLoop = resources.nextEventLoop();

    resources
        .publication(outboundChannel, STREAM_ID, options, eventLoop)
        .flatMap(
            publication ->
                resources
                    .inbound(image, null /*subscription*/, eventLoop)
                    .doOnError(ex -> publication.dispose())
                    .flatMap(inbound -> newConnection(sessionId, publication, inbound)))
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

  private Mono<? extends AeronConnection> newConnection(
      int sessionId, MessagePublication publication, DefaultAeronInbound inbound) {
    // setup cleanup hook to use it onwards
    MonoProcessor<Void> disposeHook = MonoProcessor.create();

    disposeHooks.put(sessionId, disposeHook);

    DefaultAeronOutbound outbound = new DefaultAeronOutbound(publication);

    DuplexAeronConnection connection =
        new DuplexAeronConnection(sessionId, inbound, outbound, disposeHook);

    return connection
        .start(handler)
        .doOnError(
            ex -> {
              connection.dispose();
              disposeHooks.remove(sessionId);
            });
  }

  /**
   * Disposes {@link AeronConnection} corresponding to {@link Image#sessionId()}.
   *
   * @param image source image
   */
  private void onImageUnavailable(Image image) {
    int sessionId = image.sessionId();
    MonoProcessor<Void> disposeHook = disposeHooks.remove(sessionId);
    if (disposeHook != null) {
      logger.debug("{}: server inbound became unavailable", Integer.toHexString(sessionId));
      disposeHook.onComplete();
    }
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
          List<Mono<Void>> monos = new ArrayList<>();

          // dispose server acceptor subscription
          monos.add(
              Optional.ofNullable(acceptorSubscription)
                  .map(s -> Mono.fromRunnable(s::dispose).then(s.onDispose()))
                  .orElse(Mono.empty()));

          // dispose all existing connections
          disposeHooks.values().stream().peek(MonoProcessor::onComplete).forEach(monos::add);

          return Mono.whenDelayError(monos).doFinally(s -> disposeHooks.clear());
        });
  }

  @Override
  public String toString() {
    return "AeronServerHandler" + Integer.toHexString(System.identityHashCode(this));
  }
}

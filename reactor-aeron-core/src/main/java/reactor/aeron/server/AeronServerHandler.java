package reactor.aeron.server;

import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.jctools.maps.NonBlockingHashMapLong;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.aeron.DefaultAeronConnection;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessageSubscription;
import reactor.aeron.OnDisposable;
import reactor.core.Exceptions;
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
 * serverControlPort->outbound->MDC(sessionId)->Pub(control-endpoint, sessionId)
 * </pre>
 */
final class AeronServerHandler implements FragmentHandler, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;

  private volatile MessageSubscription subscription; // server acceptor subscription

  private final NonBlockingHashMapLong<Connection> connections = new NonBlockingHashMapLong<>(32);

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
          String inboundChannel = options.inboundUri().asString();

          logger.debug("Starting {} on: {}", this, inboundChannel);

          return resources
              .subscription(
                  inboundChannel,
                  options,
                  this, /*fragmentHandler*/
                  this::onImageAvailable, /*setup new session*/
                  this::onImageUnavailable /*remove and dispose session*/)
              .doOnSuccess(subscription -> this.subscription = subscription)
              .thenReturn(this)
              .doOnSuccess(handler -> logger.debug("Started {} on: {}", this, inboundChannel))
              .doOnError(
                  ex -> {
                    logger.error("Failed to start {} on: {}", this, inboundChannel);
                    dispose();
                  });
        });
  }

  /**
   * Inner implementation of aeron's {@link FragmentHandler}. Sits on the server inbound channel and
   * serves all incoming sessions. By {@link Header#sessionId()} corresponding {@link Connection} is
   * being found and message passed there.
   */
  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    int sessionId = header.sessionId();
    Connection connection = connections.get(sessionId);

    if (connection == null) {
      logger.warn(
          "{}: received message but server connection not found (total connections: {})",
          Integer.toHexString(sessionId),
          connections.size());
      return;
    }

    DefaultAeronInbound inbound = (DefaultAeronInbound) connection.inbound();
    inbound.onFragment(buffer, offset, length, header);
  }

  /**
   * Setting up new {@link Connection} identified by {@link Image#sessionId()}. Specifically creates
   * message publication (aeron {@link io.aeron.Publication} underneath) with control-endpoint,
   * control-mode and given sessionId. Essentially creates server side MDC for concrete sessionId;
   * think of this as <i>server-side-individual-MDC</i>.
   *
   * @param image source image
   */
  private void onImageAvailable(Image image) {
    // Pub(control-endpoint{address:serverControlPort}, sessionId)->MDC(sessionId)
    int sessionId = image.sessionId();
    String outboundChannel = options.outboundUri().sessionId(sessionId).asString();

    // not expecting following condition be true at normal circumstances; passing it would mean
    // aeron changed contract/semantic around sessionId
    if (connections.containsKey(sessionId)) {
      logger.error("{}: server connection already exists!?", Integer.toHexString(sessionId));
      return;
    }

    logger.debug(
        "{}: creating server connection: {}", Integer.toHexString(sessionId), outboundChannel);

    resources
        .publication(outboundChannel, options)
        .map(
            publication -> {
              DefaultAeronInbound inbound = new DefaultAeronInbound();
              DefaultAeronOutbound outbound = new DefaultAeronOutbound(publication);
              return new DefaultAeronConnection(sessionId, inbound, outbound, publication);
            })
        .doOnSuccess(connection -> setupConnection(sessionId, connection))
        .subscribe(
            null,
            ex ->
                logger.warn(
                    "{}: failed to create server outbound, cause: {}",
                    Integer.toHexString(sessionId),
                    ex.toString()),
            () ->
                logger.debug(
                    "{}: created server connection: {}",
                    Integer.toHexString(sessionId),
                    outboundChannel));
  }

  /**
   * Disposes {@link Connection} corresponding to {@link Image#sessionId()}.
   *
   * @param image source image
   */
  private void onImageUnavailable(Image image) {
    int sessionId = image.sessionId();
    Connection connection = connections.remove(sessionId);

    if (connection != null) {
      logger.debug("{}: server inbound became unavailable", Integer.toHexString(sessionId));
      connection.dispose();
      connection
          .onDispose()
          .doFinally(
              s -> logger.debug("{}: server connection disposed", Integer.toHexString(sessionId)))
          .subscribe(
              null,
              th -> {
                // no-op
              });
    } else {
      logger.debug(
          "{}: attempt to remove server connection but it wasn't found (total connections: {})",
          Integer.toHexString(sessionId),
          connections.size());
    }
  }

  private void setupConnection(int sessionId, DefaultAeronConnection connection) {
    // store
    connections.put(sessionId, connection);

    // register cleanup hook
    connection
        .onDispose()
        .doFinally(s -> connections.remove(sessionId))
        .subscribe(
            null,
            th -> {
              // no-op
            });

    if (handler == null) {
      logger.warn("{}: handler function is not specified", Integer.toHexString(sessionId));
      return;
    }

    try {
      if (!connection.isDisposed()) {
        handler.apply(connection).subscribe(connection.disposeSubscriber());
      }
    } catch (Exception ex) {
      logger.error(
          "{}: unexpected exception occurred on handler.apply(), cause: ",
          Integer.toHexString(sessionId),
          ex);
      connection.dispose();
      throw Exceptions.propagate(ex);
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
              Optional.ofNullable(subscription)
                  .map(s -> Mono.fromRunnable(s::dispose).then(s.onDispose()))
                  .orElse(Mono.empty()));

          // dispose all existing connections
          connections
              .values()
              .stream()
              .peek(Connection::dispose)
              .map(Connection::onDispose)
              .forEach(monos::add);

          return Mono.whenDelayError(monos).doFinally(s -> connections.clear());
        });
  }

  @Override
  public String toString() {
    return "AeronServerHandler" + Integer.toHexString(System.identityHashCode(this));
  }
}

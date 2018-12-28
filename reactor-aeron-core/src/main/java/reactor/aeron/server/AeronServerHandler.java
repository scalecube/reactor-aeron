package reactor.aeron.server;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronOutbound;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronUtils;
import reactor.aeron.Connection;
import reactor.aeron.ControlMessageSubscriber;
import reactor.aeron.DefaultAeronInbound;
import reactor.aeron.DefaultAeronOutbound;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.OnDisposable;
import reactor.aeron.Protocol;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

final class AeronServerHandler implements ControlMessageSubscriber, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronServerHandler.class);

  private static final AtomicInteger streamIdCounter = new AtomicInteger(1000);

  private final String category;
  private final AeronOptions options;
  private final AeronResources resources;
  private final Function<? super Connection, ? extends Publisher<Void>> handler;
  private final String serverChannel;

  private final AtomicLong nextSessionId = new AtomicLong(0);

  private final List<SessionHandler> handlers = new CopyOnWriteArrayList<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  AeronServerHandler(AeronServerSettings settings) {
    category = settings.name();
    options = settings.options();
    resources = settings.aeronResources();
    handler = settings.handler();
    serverChannel = options.serverChannel();

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(null, th -> logger.warn("AeronServerHandler disposed with error: " + th));
  }

  @Override
  public void onSubscription(Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onConnect(
      long connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "[{}] Received {} for connectRequestId: {}, channel={}, "
              + "clientControlStreamId={}, clientSessionStreamId={}",
          category,
          MessageType.CONNECT,
          connectRequestId,
          AeronUtils.minifyChannel(clientChannel),
          clientControlStreamId,
          clientSessionStreamId);
    }

    int serverSessionStreamId = streamIdCounter.incrementAndGet();
    long sessionId = nextSessionId.incrementAndGet();
    SessionHandler sessionHandler =
        new SessionHandler(
            clientChannel,
            clientSessionStreamId,
            clientControlStreamId,
            connectRequestId,
            sessionId,
            serverSessionStreamId);

    sessionHandler
        .start()
        .subscribe(
            connection ->
                handler
                    .apply(connection) //
                    .subscribe(connection.disposeSubscriber()),
            th ->
                logger.error(
                    "[{}] Occurred exception on connect to {}, sessionId: {}, "
                        + "connectRequestId: {}, clientSessionStreamId: {}, "
                        + "clientControlStreamId: {}, serverSessionStreamId: {}, error: ",
                    category,
                    clientChannel,
                    sessionId,
                    connectRequestId,
                    clientSessionStreamId,
                    clientControlStreamId,
                    serverSessionStreamId,
                    th));
  }

  @Override
  public void onConnectAck(long connectRequestId, long sessionId, int serverSessionStreamId) {
    logger.error(
        "CONNECT_ACK is not supported, connectRequestId: {}",
        connectRequestId,
        sessionId,
        serverSessionStreamId);
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
        () ->
            Mono.whenDelayError(
                handlers
                    .stream()
                    .map(
                        sessionHandler -> {
                          sessionHandler.dispose();
                          return sessionHandler.onDispose();
                        })
                    .collect(Collectors.toList())));
  }

  private class SessionHandler implements Connection {

    private final Logger logger = LoggerFactory.getLogger(SessionHandler.class);

    private final DefaultAeronOutbound outbound;
    private final DefaultAeronInbound inbound;
    private final String clientChannel;
    private final int clientSessionStreamId;
    private final int serverSessionStreamId;
    private final long connectRequestId;
    private final long sessionId;
    private final int clientControlStreamId;

    private final Mono<MessagePublication> controlPublication;

    private final MonoProcessor<Void> dispose = MonoProcessor.create();
    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private SessionHandler(
        String clientChannel,
        int clientSessionStreamId,
        int clientControlStreamId,
        long connectRequestId,
        long sessionId,
        int serverSessionStreamId) {
      this.clientSessionStreamId = clientSessionStreamId;
      this.clientChannel = clientChannel;
      this.outbound = new DefaultAeronOutbound(category, clientChannel, resources, options);
      this.connectRequestId = connectRequestId;
      this.sessionId = sessionId;
      this.serverSessionStreamId = serverSessionStreamId;
      this.clientControlStreamId = clientControlStreamId;
      this.inbound = new DefaultAeronInbound(category, resources);

      this.controlPublication =
          Mono.defer(() -> newControlPublication(clientChannel, clientControlStreamId)).cache();

      this.dispose
          .then(doDispose())
          .doFinally(s -> onDispose.onComplete())
          .subscribe(null, th -> logger.warn("SessionHandler disposed with error: " + th));
    }

    private Mono<MessagePublication> newControlPublication(
        String clientChannel, int clientControlStreamId) {
      return resources.messagePublication(
          category, clientChannel, clientControlStreamId, options, resources.nextEventLoop());
    }

    private Mono<? extends Connection> start() {
      return connect()
          .then(outbound.start(clientSessionStreamId))
          .then(inbound.start(serverChannel, serverSessionStreamId, this::dispose))
          .thenReturn(this)
          .doOnSuccess(connection -> handlers.add(this))
          .doOnError(
              ex -> {
                logger.error("Exception occurred on: {}, cause: {}", this, ex.toString());
                dispose();
              });
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
    public String toString() {
      return "ServerSession{"
          + "category="
          + category
          + ", sessionId="
          + sessionId
          + ", clientChannel="
          + AeronUtils.minifyChannel(clientChannel)
          + ", serverChannel="
          + AeronUtils.minifyChannel(serverChannel)
          + ", clientControlStreamId="
          + clientControlStreamId
          + ", clientSessionStreamId="
          + clientSessionStreamId
          + ", serverSessionStreamId="
          + serverSessionStreamId
          + '}';
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
            logger.debug("About to close {}", this);

            handlers.remove(this);

            Optional.ofNullable(outbound) //
                .ifPresent(DefaultAeronOutbound::dispose);
            Optional.ofNullable(inbound) //
                .ifPresent(DefaultAeronInbound::dispose);

            return Mono.whenDelayError(
                    Optional.ofNullable(outbound)
                        .map(DefaultAeronOutbound::onDispose)
                        .orElse(Mono.empty()),
                    Optional.ofNullable(inbound)
                        .map(DefaultAeronInbound::onDispose)
                        .orElse(Mono.empty()))
                .doFinally(s -> logger.debug("Closed {}", this));
          });
    }

    private Mono<Void> connect() {
      return Mono.defer(
          () -> {
            Duration retryInterval = Duration.ofMillis(100);
            Duration connectTimeout = options.connectTimeout().plus(options.backpressureTimeout());
            long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();

            return controlPublication.flatMap(
                publication ->
                    sendConnectAck(publication)
                        .retryBackoff(retryCount, retryInterval, retryInterval)
                        .timeout(connectTimeout)
                        .then()
                        .doOnSuccess(
                            avoid ->
                                logger.debug("ServerSession sent CONNECT_ACK to: {}", publication))
                        .onErrorResume(
                            th -> {
                              logger.warn(
                                  "Failed to send CONNECT_ACK to: {}, cause: {}",
                                  publication,
                                  th.toString());
                              return Mono.error(
                                  new RuntimeException("Failed to send CONNECT_ACK", th));
                            }));
          });
    }

    private Mono<Void> sendConnectAck(MessagePublication publication) {
      return publication.enqueue(
          Protocol.createConnectAckBody(sessionId, connectRequestId, serverSessionStreamId));
    }
  }
}

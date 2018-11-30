package reactor.aeron.server;

import io.aeron.Publication;
import java.time.Duration;
import java.util.UUID;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.DefaultMessagePublication;
import reactor.aeron.MessagePublication;
import reactor.aeron.MessageType;
import reactor.aeron.Protocol;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public class ServerConnector implements Disposable {

  private static final Logger logger = Loggers.getLogger(ServerConnector.class);

  private final String category;

  private final MessagePublication controlMessagePublication;

  private final int serverSessionStreamId;

  private final UUID connectRequestId;

  private final AeronOptions options;

  private final long sessionId;

  ServerConnector(
      String category,
      AeronResources aeronResources,
      String clientChannel,
      int clientControlStreamId,
      long sessionId,
      int serverSessionStreamId,
      UUID connectRequestId,
      AeronOptions options) {
    this.category = category;
    this.serverSessionStreamId = serverSessionStreamId;
    this.connectRequestId = connectRequestId;
    this.options = options;
    this.sessionId = sessionId;
    Publication clientControlPublication =
        aeronResources.publication(
            category,
            clientChannel,
            clientControlStreamId,
            "to send control requests to client",
            sessionId);

    this.controlMessagePublication =
        new DefaultMessagePublication(
            aeronResources.eventLoop(), clientControlPublication, category, options);
  }

  Mono<Void> connect() {
    long retryMillis = 100;
    long timeoutMillis =
        options.connectTimeoutMillis() + options.controlBackpressureTimeoutMillis();
    long retryCount = timeoutMillis / retryMillis;

    return controlMessagePublication
        .enqueue(
            MessageType.CONNECT_ACK,
            Protocol.createConnectAckBody(connectRequestId, serverSessionStreamId),
            sessionId)
        .retryBackoff(retryCount, Duration.ofMillis(retryMillis), Duration.ofMillis(retryMillis))
        .timeout(Duration.ofMillis(timeoutMillis))
        .doOnSuccess(
            avoid ->
                logger.debug("[{}] Sent {} to {}", category, MessageType.CONNECT_ACK, category))
        .onErrorResume(
            throwable -> {
              String errMessage =
                  String.format(
                      "Failed to send %s into %s",
                      MessageType.CONNECT_ACK, controlMessagePublication);
              return Mono.error(new RuntimeException(errMessage, throwable));
            });
  }

  @Override
  public void dispose() {
    controlMessagePublication.close();
  }
}

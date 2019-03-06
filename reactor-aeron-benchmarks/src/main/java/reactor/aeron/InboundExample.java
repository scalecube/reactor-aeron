package reactor.aeron;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InboundExample {

  private static final Logger logger = LoggerFactory.getLogger(InboundExample.class);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(Configurations.FRAGMENT_COUNT_LIMIT)
            .singleWorker()
            .workerIdleStrategySupplier(Configurations::idleStrategy)
            .start()
            .block();

    AeronChannelUriString inboundUri =
        new AeronChannelUriString()
            .uri(options -> options.media("udp").endpoint("localhost:14000").reliable(true));
    int inboundStreamId = 123;

    logger.info("inboundUri: {}, streamId: {}", inboundUri.asString(), inboundStreamId);

    resources
        .inbound(
            new AeronOptions()
                .inboundStreamId(inboundStreamId)
                .inboundUri(inboundUri)
                .onImageAvailable(
                    image ->
                        logger.info(
                            "image is available, sessionId: {}, channel: {}",
                            image.sessionId(),
                            image.subscription().channel()))
                .onImageUnavailable(
                    image ->
                        logger.info(
                            "image is unavailable, sessionId: {}, channel: {}",
                            image.sessionId(),
                            image.subscription().channel())))
        .map(AeronInbound::receive)
        .flatMapMany(DirectBufferFlux::asString)
        .log("receive ")
        .blockLast();

    resources.dispose();
  }
}

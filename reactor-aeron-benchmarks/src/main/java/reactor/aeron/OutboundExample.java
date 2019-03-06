package reactor.aeron;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class OutboundExample {

  private static final Logger logger = LoggerFactory.getLogger(OutboundExample.class);

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

    AeronChannelUriString outboundUri =
        new AeronChannelUriString()
            .uri(options -> options.media("udp").endpoint("localhost:14000").reliable(true));
    int outboundStreamId = 123;

    logger.info("outboundUri: {}, streamId: {}", outboundUri.asString(), outboundStreamId);

    resources
        .outbound(new AeronOptions().outboundUri(outboundUri).outboundStreamId(outboundStreamId))
        .flatMap(
            outbound ->
                outbound
                    .sendString(
                        Flux.interval(Duration.ofSeconds(1))
                            .map(i -> "Hello from outbound " + i)
                            .log("send "))
                    .then())
        .block();

    resources.dispose();
  }
}

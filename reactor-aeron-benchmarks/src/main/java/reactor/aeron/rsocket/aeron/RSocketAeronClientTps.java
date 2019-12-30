package reactor.aeron.rsocket.aeron;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import reactor.aeron.Configurations;
import reactor.aeron.RateReporter;
import reactor.aeron.mdc.AeronResources;

public final class RSocketAeronClientTps {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    printSettings();

    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(Configurations.FRAGMENT_COUNT_LIMIT)
            .singleWorker()
            .workerIdleStrategySupplier(Configurations::idleStrategy)
            .start()
            .block();

    RSocket client = null;
    // todo io.rsocket.transport.ClientTransport was changed
    // RSocketFactory.connect()
    //     .frameDecoder(PayloadDecoder.ZERO_COPY)
    //     .transport(
    //         () ->
    //             new AeronClientTransport(
    //                 AeronClient.create(resources)
    //                     .options(
    //                         Configurations.MDC_ADDRESS,
    //                         Configurations.MDC_PORT,
    //                         Configurations.MDC_CONTROL_PORT)))
    //     .start()
    //     .block();

    RateReporter reporter = new RateReporter();

    Payload request = ByteBufPayload.create("hello");

    client
        .requestStream(request)
        .doOnNext(
            payload -> {
              reporter.onMessage(1, payload.sliceData().readableBytes());
              payload.release();
            })
        .doOnError(Throwable::printStackTrace)
        .doFinally(s -> reporter.dispose())
        .then()
        .block();
  }

  private static void printSettings() {
    System.out.println(
        "address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT
            + ", controlPort: "
            + Configurations.MDC_CONTROL_PORT);
    System.out.println("pollFragmentLimit of " + Configurations.FRAGMENT_COUNT_LIMIT);
    System.out.println(
        "Using worker idle strategy "
            + Configurations.idleStrategy().getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");
  }
}

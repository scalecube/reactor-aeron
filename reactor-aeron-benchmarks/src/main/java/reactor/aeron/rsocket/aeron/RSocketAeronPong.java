package reactor.aeron.rsocket.aeron;

import reactor.aeron.Configurations;
import reactor.aeron.mdc.AeronResources;

public final class RSocketAeronPong {

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

    // todo io.rsocket.transport.ServerTransport was changed
    // RSocketFactory.receive()
    //     .frameDecoder(PayloadDecoder.ZERO_COPY)
    //     .acceptor(
    //         (setupPayload, rsocket) ->
    //             Mono.just(
    //                 new AbstractRSocket() {
    //                   @Override
    //                   public Mono<Payload> requestResponse(Payload payload) {
    //                     return Mono.just(payload);
    //                   }
    //                 }))
    //     .transport(
    //         new AeronServerTransport(
    //             AeronServer.create(resources)
    //                 .options(
    //                     Configurations.MDC_ADDRESS,
    //                     Configurations.MDC_PORT,
    //                     Configurations.MDC_CONTROL_PORT)))
    //     .start()
    //     .block()
    //     .onClose()
    //     .doFinally(s -> resources.dispose())
    //     .then(resources.onDispose())
    //     .block();
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

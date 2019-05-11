package reactor.aeron;

public final class AeronPongServer {

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

    AeronServer.create(resources)
        .options(
            Configurations.MDC_ADDRESS, Configurations.MDC_PORT, Configurations.MDC_CONTROL_PORT)
        .handle(
            connection ->
                connection
                    .outbound()
                    .send(connection.inbound().receive())
                    .then(connection.onDispose()))
        .bind()
        .block()
        .onDispose(resources)
        .onDispose()
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

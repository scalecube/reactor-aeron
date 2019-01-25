package reactor.aeron.demo;

import org.agrona.concurrent.BusySpinIdleStrategy;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

public final class AeronPongServerSingleMediaDriver {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    AeronResources resources =
        new AeronResources()
            .pollFragmentLimit(8)
            .aeron(ctx -> ctx.aeronDirectoryName("/tmp/aeron-SingleMediaDriver"))
            .workerIdleStrategySupplier(BusySpinIdleStrategy::new)
            .singleWorker()
            .start()
            .block();

    AeronServer.create(resources)
        .options("localhost", 13000, 13001)
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
}

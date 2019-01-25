package reactor.aeron.demo;

import org.agrona.concurrent.BusySpinIdleStrategy;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

public final class AeronPongServerSingleMD {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .aeron(
                ctx ->
                    ctx.aeronDirectoryName(
                        "/tmp/aeron-serhiihabryiel-f96de376-6443-4a73-9184-7a91a72263b2"))
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

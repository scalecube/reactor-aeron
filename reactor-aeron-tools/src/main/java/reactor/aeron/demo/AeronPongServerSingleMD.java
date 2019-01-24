package reactor.aeron.demo;

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
                        "/var/folders/tx/11bk01r93rv4nhblfzfpmdhr0000gn/T/aeron-segabriel-bd072f46-5cfe-49ee-b10f-09a1d3ad7f1a"))
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

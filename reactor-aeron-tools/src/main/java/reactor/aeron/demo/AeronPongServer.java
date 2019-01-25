package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

public final class AeronPongServer {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .media(ctx -> ctx.threadingMode(ThreadingMode.SHARED))
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

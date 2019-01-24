package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

public class ServerThroughputSingleMD {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources aeronResources =
        new AeronResources()
            .useTmpDir()
            .aeron(
                ctx ->
                    ctx.aeronDirectoryName(
                        "/var/folders/tx/11bk01r93rv4nhblfzfpmdhr0000gn/T/aeron-segabriel-1df4c987-3526-44d1-a11d-c6697b5fc9fc"))
            .start()
            .block();

    RateReporter reporter = new RateReporter(Duration.ofSeconds(1));

    AeronServer.create(aeronResources)
        .options("localhost", 13000, 13001)
        .handle(
            connection ->
                connection
                    .inbound()
                    .receive()
                    .doOnNext(buffer -> reporter.onMessage(1, buffer.capacity()))
                    .then(connection.onDispose()))
        .bind()
        .block()
        .onDispose()
        .doFinally(
            s -> {
              reporter.dispose();
              aeronResources.dispose();
            })
        .then(aeronResources.onDispose())
        .block();
  }
}

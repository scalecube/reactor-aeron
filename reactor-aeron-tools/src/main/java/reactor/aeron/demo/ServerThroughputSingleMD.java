package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.agrona.concurrent.BusySpinIdleStrategy;
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
//            .aeron(ctx -> ctx.idleStrategy(new BusySpinIdleStrategy()))
            .aeron(
                ctx ->
                    ctx.aeronDirectoryName(
                        "/tmp/aeron-serhiihabryiel-c6cad9cc-7816-40d7-a792-eb00124e8a77"))
            .workerIdleStrategySupplier(BusySpinIdleStrategy::new)
            .singleWorker()
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

package reactor.aeron.demo;

import static io.aeron.driver.Configuration.IDLE_MAX_PARK_NS;
import static io.aeron.driver.Configuration.IDLE_MAX_SPINS;
import static io.aeron.driver.Configuration.IDLE_MAX_YIELDS;
import static io.aeron.driver.Configuration.IDLE_MIN_PARK_NS;

import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;

public class ServerThroughput {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    IdleReporter idleReporter = new IdleReporter(Duration.ofSeconds(1));

    AeronResources aeronResources =
        new AeronResources()
            .workerIdleStrategySupplier(
                () ->
                    new BackoffIdleStrategyWithReporter(
                        IDLE_MAX_SPINS,
                        IDLE_MAX_YIELDS,
                        IDLE_MIN_PARK_NS,
                        IDLE_MAX_PARK_NS,
                        idleReporter))
            .useTmpDir()
            .media(ctx -> ctx.threadingMode(ThreadingMode.SHARED))
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

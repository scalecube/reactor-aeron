package reactor.aeron.demo;

import static io.aeron.driver.Configuration.IDLE_MAX_PARK_NS;
import static io.aeron.driver.Configuration.IDLE_MAX_SPINS;
import static io.aeron.driver.Configuration.IDLE_MAX_YIELDS;
import static io.aeron.driver.Configuration.IDLE_MIN_PARK_NS;

import io.aeron.driver.ThreadingMode;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;

public class ClientThroughput {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    IdleReporter idleReporter = new IdleReporter(Duration.ofSeconds(1));

    AeronResources aeronResources =
        new AeronResources()
            .useTmpDir()
            .workerIdleStrategySupplier(
                () ->
                    new BackoffIdleStrategyWithReporter(
                        IDLE_MAX_SPINS,
                        IDLE_MAX_YIELDS,
                        IDLE_MIN_PARK_NS,
                        IDLE_MAX_PARK_NS,
                        idleReporter))
            .media(ctx -> ctx.threadingMode(ThreadingMode.SHARED))
            .start()
            .block();

    DirectBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

    AeronClient.create(aeronResources)
        .options("localhost", 13000, 13001)
        .handle(
            connection ->
                connection
                    .outbound()
                    .send(Flux.range(0, Integer.MAX_VALUE).map(i -> buffer))
                    .then(connection.onDispose()))
        .connect()
        .block()
        .onDispose()
        .doFinally(s -> aeronResources.dispose())
        .then(aeronResources.onDispose())
        .block();
  }
}

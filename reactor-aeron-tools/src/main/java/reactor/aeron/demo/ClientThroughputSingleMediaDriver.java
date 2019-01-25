package reactor.aeron.demo;

import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;

public class ClientThroughputSingleMediaDriver {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources aeronResources =
        new AeronResources()
            .pollFragmentLimit(8)
            .aeron(ctx -> ctx.aeronDirectoryName("/tmp/aeron-SingleMediaDriver"))
            .workerIdleStrategySupplier(BusySpinIdleStrategy::new)
            .singleWorker()
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

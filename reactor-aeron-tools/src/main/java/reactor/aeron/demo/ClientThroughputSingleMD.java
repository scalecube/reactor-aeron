package reactor.aeron.demo;

import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;

public class ClientThroughputSingleMD {

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

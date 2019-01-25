package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import java.nio.ByteBuffer;
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
    AeronResources aeronResources =
        new AeronResources()
            .useTmpDir()
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

package reactor.aeron.demo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Random;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;

public class ClientThroughput {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = AeronResources.start();
    try {
      byte[] bytes = new byte[1024];
      Random random = new Random();
      random.nextBytes(bytes);

      ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(bytes.length);
      byteBuf.writeBytes(bytes);

      AeronClient.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .outbound()
                      .send(
                          Flux.create(
                              sink -> {
                                System.out.println("About to send");
                                for (int i = 0; i < 10_000 * 1024; i++) {
                                  sink.next(byteBuf.retainedSlice());
                                }
                                sink.complete();
                                System.out.println("Send complete");
                              }))
                      .then(connection.onDispose()))
          .connect()
          .block();

      System.out.println("main completed");
      Thread.currentThread().join();
    } finally {
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}

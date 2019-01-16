package reactor.aeron.demo;

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
      byte[] bytes = new byte[128];
      Random random = new Random();
      random.nextBytes(bytes);
      String msg = new String(bytes);

      AeronClient.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .outbound()
                      .sendString(
                          Flux.create(
                              sink -> {
                                System.out.println("About to send");
                                for (int i = 0; i < 10_000 * 1024; i++) {
                                  sink.next(msg);
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

package reactor.aeron.demo;

import java.time.Duration;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.core.publisher.Flux;

public class ServerServerSends {

  static {
    System.setProperty("io.netty.leakDetection.level", "PARANOID");
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = AeronResources.start();
    try {
      AeronServer.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .outbound()
                      .sendString(
                          Flux.range(1, 10000)
                              .delayElements(Duration.ofMillis(250))
                              .map(String::valueOf)
                              .log("send"))
                      .then(connection.onDispose()))
          .bind()
          .block();

      System.out.println("main finished");
      Thread.currentThread().join();
    } finally {
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}

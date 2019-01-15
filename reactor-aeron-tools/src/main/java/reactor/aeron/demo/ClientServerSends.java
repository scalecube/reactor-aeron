package reactor.aeron.demo;

import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;

public class ClientServerSends {

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
      AeronClient.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .inbound()
                      .receive()
                      .asString()
                      .log("receive")
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

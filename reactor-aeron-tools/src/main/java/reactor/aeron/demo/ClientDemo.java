package reactor.aeron.demo;

import java.util.Objects;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronConnection;
import reactor.aeron.AeronResources;
import reactor.aeron.ByteBufFlux;

public class ClientDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronConnection connection = null;
    AeronResources aeronResources = AeronResources.start();
    try {
      connection =
          AeronClient.create(aeronResources)
              .options("localhost", 13000, 13001)
              .handle(
                  connection1 -> {
                    System.out.println("Handler invoked");
                    return connection1
                        .outbound()
                        .send(ByteBufFlux.fromString("Hello", "world!").log("send"))
                        .then(connection1.onDispose());
                  })
              .connect()
              .block();
    } finally {
      Objects.requireNonNull(connection).dispose();
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
    System.out.println("main completed");
  }
}

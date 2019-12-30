package reactor.aeron;

import java.util.stream.Stream;
import reactor.aeron.mdc.AeronClient;
import reactor.aeron.mdc.AeronResources;
import reactor.core.publisher.Flux;

public class ClientDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources resources = new AeronResources().useTmpDir().start().block();

    AeronClient.create(resources)
        .options("localhost", 13000, 13001)
        .handle(
            connection1 -> {
              System.out.println("Handler invoked");
              return connection1
                  .outbound()
                  .sendString(Flux.fromStream(Stream.of("Hello", "world!")).log("send"))
                  .then(connection1.onDispose());
            })
        .connect()
        .block()
        .onDispose(resources)
        .onDispose()
        .block();
  }
}

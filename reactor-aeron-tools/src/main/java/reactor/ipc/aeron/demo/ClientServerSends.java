package reactor.ipc.aeron.demo;

import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.client.AeronClient;

public class ClientServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronClient client =
        AeronClient.create(
            "client",
            options -> {
              options.serverChannel("aeron:udp?endpoint=localhost:13000");
              options.clientChannel("aeron:udp?endpoint=localhost:12001");
            });
    client
        .newHandler(
            (inbound, outbound) -> {
              System.out.println("Handler invoked");
              inbound.receive().asString().log("client receive -> ")
                  .doOnError(Throwable::printStackTrace)
                  .subscribe();


              outbound
                  .send(
                      Flux.range(1, 10000)
                          .delayElements(Duration.ofSeconds(3))
                          .map(i -> AeronUtils.stringToByteBuffer("" + i))
                          .log("client send -> "))
                  .then()
                  .subscribe();

              return Mono.never();
            })
        .block();

    System.out.println("main completed");
  }
}

package reactor.ipc.aeron.demo;

import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.aeron.AeronUtils;
import reactor.ipc.aeron.server.AeronServer;

public class ServerServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronServer server =
        AeronServer.create(
            "server",
            options -> {
              options.serverChannel("aeron:udp?endpoint=localhost:13000");
            });
    server
        .newHandler(
            (inbound, outbound) -> {
              inbound.receive().asString().log("server receive -> ").subscribe();

              outbound
                  .send(
                      Flux.range(1, 10000)
                          .delayElements(Duration.ofMillis(250))
                          .map(i -> AeronUtils.stringToByteBuffer("" + i))
                          .log("server send -> "))
                  .then()
                  .subscribe();
              return Mono.never();
            })
        .block();

    System.out.println("main finished");
  }
}

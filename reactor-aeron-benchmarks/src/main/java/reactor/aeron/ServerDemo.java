package reactor.aeron;

import static reactor.aeron.DefaultFragmentMapper.asString;

import org.agrona.DirectBuffer;

public class ServerDemo {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources resources = new AeronResources().useTmpDir().start().block();

    AeronServer.create(resources)
        .options("localhost", 13000, 13001)
        .handle(
            connection ->
                connection
                    .<DirectBuffer>inbound()
                    .receive()
                    .map(asString())
                    .log("receive")
                    .then(connection.onDispose()))
        .bind()
        .block()
        .onDispose(resources)
        .onDispose()
        .block();
  }
}

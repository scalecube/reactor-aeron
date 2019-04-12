package reactor.aeron.rsocket.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.ZeroCopyPayloadDecoder;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Random;
import reactor.aeron.Configurations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

public final class RSocketNettyServerTps {

  private static final ByteBuf BUFFER =
      ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH);

  static {
    Random random = new Random(System.nanoTime());
    byte[] bytes = new byte[Configurations.MESSAGE_LENGTH];
    random.nextBytes(bytes);
    BUFFER.writeBytes(bytes);
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    System.out.println(
        "message size: "
            + Configurations.MESSAGE_LENGTH
            + ", number of messages: "
            + Configurations.NUMBER_OF_MESSAGES
            + ", address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT);

    LoopResources loopResources = LoopResources.create("rsocket-netty");

    TcpServer tcpServer =
        TcpServer.create()
            .runOn(loopResources)
            .host(Configurations.MDC_ADDRESS)
            .port(Configurations.MDC_PORT)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .doOnConnection(System.out::println);

    RSocketFactory.receive()
        .frameDecoder(new ZeroCopyPayloadDecoder())
        .acceptor(
            (setupPayload, rsocket) -> {
              System.out.println(rsocket);
              return Mono.just(
                  new AbstractRSocket() {
                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                      payload.release();

                      long msgNum = Configurations.NUMBER_OF_MESSAGES;
                      System.out.println("streaming " + msgNum + " messages ...");

                      return Flux.range(0, Integer.MAX_VALUE)
                          .map(i -> ByteBufPayload.create(BUFFER.retainedSlice()));
                    }
                  });
            })
        .transport(() -> TcpServerTransport.create(tcpServer))
        .start()
        .block()
        .onClose()
        .block();
  }
}

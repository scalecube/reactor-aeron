package reactor.aeron.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.util.Random;
import reactor.aeron.Configurations;
import reactor.core.publisher.Flux;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public class ReactorNettyClientTps {

  private static final ByteBuf PAYLOAD =
      ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH);

  static {
    Random random = new Random(System.nanoTime());
    byte[] bytes = new byte[Configurations.MESSAGE_LENGTH];
    random.nextBytes(bytes);
    PAYLOAD.writeBytes(bytes);
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    System.out.println(
        "message size: "
            + Configurations.MESSAGE_LENGTH
            + ", number of messages: "
            + Configurations.NUMBER_OF_MESSAGES
            + ", address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT);

    LoopResources loopResources = LoopResources.create("reactor-netty");

    TcpClient.create(ConnectionProvider.newConnection())
        .runOn(loopResources)
        .host(Configurations.MDC_ADDRESS)
        .port(Configurations.MDC_PORT)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .doOnConnected(System.out::println)
        .bootstrap(
            b ->
                BootstrapHandlers.updateConfiguration(
                    b,
                    "channel",
                    (connectionObserver, channel) -> {
                      setupChannel(channel);
                    }))
        .handle(
            (inbound, outbound) -> {
              int msgNum = (int) Configurations.NUMBER_OF_MESSAGES;
              System.out.println("streaming " + msgNum + " messages ...");

              return outbound.send(Flux.range(0, msgNum).map(i -> PAYLOAD.retainedSlice())).then();
            })
        .connectNow()
        .onDispose()
        .doFinally(s -> loopResources.dispose())
        .block();
  }

  private static void setupChannel(Channel channel) {
    final int maxFrameLength = 1024 * 1024;
    final int lengthFieldLength = 2;

    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addLast(new LengthFieldPrepender(lengthFieldLength));
    pipeline.addLast(
        new LengthFieldBasedFrameDecoder(
            maxFrameLength, 0, lengthFieldLength, 0, lengthFieldLength));
  }
}

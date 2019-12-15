package reactor.aeron.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.agrona.console.ContinueBarrier;
import reactor.aeron.Configurations;
import reactor.aeron.LatencyReporter;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public class ReactorNettyClientPing {

  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);

  private static final LatencyReporter latencyReporter = new LatencyReporter(HISTOGRAM);

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

    Connection connection =
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
            .connectNow();

    ContinueBarrier barrier = new ContinueBarrier("Execute again?");
    do {
      System.out.println("Pinging " + Configurations.NUMBER_OF_MESSAGES + " messages");
      roundTripMessages(connection, Configurations.NUMBER_OF_MESSAGES);
      System.out.println("Histogram of RTT latencies in microseconds.");
    } while (barrier.await());

    connection.dispose();

    connection.onDispose(loopResources).onDispose().block();
  }

  private static void roundTripMessages(Connection connection, long count) {
    HISTOGRAM.reset();

    Disposable disp = latencyReporter.start();

    connection
        .outbound()
        .sendObject(Flux.range(0, Configurations.REQUESTED))
        .then()
        .subscribe();

    connection
        .outbound()
        .sendObject(
            connection
                .inbound()
                .receive()
                .retain()
                .take(count)
                .doOnNext(
                    buffer -> {
                      long start = buffer.readLong();
                      buffer.readerIndex(Configurations.MESSAGE_LENGTH);
                      long diff = System.nanoTime() - start;
                      HISTOGRAM.recordValue(diff);
                      buffer.release();
                    })
                .map(buffer -> 1))
        .then(
            Mono.defer(
                () -> Mono.delay(Duration.ofMillis(100)).doOnSubscribe(s -> disp.dispose()).then()))
        .then()
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
    pipeline.addLast(
        new MessageToByteEncoder<Integer>() {
          @Override
          protected void encode(ChannelHandlerContext ctx, Integer msg, ByteBuf out) {
            out.writeLong(System.nanoTime());
            out.writeBytes(PAYLOAD.slice());
          }
        });
  }
}

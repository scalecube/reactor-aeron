package reactor.aeron.rsocket.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.ZeroCopyPayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.HdrHistogram.Recorder;
import reactor.aeron.Configurations;
import reactor.aeron.LatencyReporter;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public final class RSocketNettyPing {

  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);
  private static final LatencyReporter latencyReporter = new LatencyReporter(HISTOGRAM);

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

    TcpClient tcpClient =
        TcpClient.create(ConnectionProvider.newConnection())
            .runOn(loopResources)
            .host(Configurations.MDC_ADDRESS)
            .port(Configurations.MDC_PORT)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .doOnConnected(System.out::println);

    RSocket client =
        RSocketFactory.connect()
            .frameDecoder(new ZeroCopyPayloadDecoder())
            .transport(() -> TcpClientTransport.create(tcpClient))
            .start()
            .doOnSuccess(System.out::println)
            .block();

    Disposable report = latencyReporter.start();

    Supplier<Payload> payloadSupplier = () -> ByteBufPayload.create(BUFFER.retainedSlice());

    Flux.range(1, (int) Configurations.NUMBER_OF_MESSAGES)
        .flatMap(
            i -> {
              long start = System.nanoTime();
              return client
                  .requestResponse(payloadSupplier.get())
                  .doOnNext(Payload::release)
                  .doFinally(
                      signalType -> {
                        long diff = System.nanoTime() - start;
                        HISTOGRAM.recordValue(diff);
                      });
            },
            64)
        .doOnError(Throwable::printStackTrace)
        .doOnTerminate(
            () -> System.out.println("Sent " + Configurations.NUMBER_OF_MESSAGES + " messages"))
        .doFinally(s -> report.dispose())
        .then()
        .block();
  }
}

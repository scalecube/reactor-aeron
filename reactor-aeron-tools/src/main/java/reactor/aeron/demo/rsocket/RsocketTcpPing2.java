package reactor.aeron.demo.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import reactor.aeron.demo.Configurations;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public final class RsocketTcpPing2 {

  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);
  private static final Payload PAYLOAD =
      ByteBufPayload.create(ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH));

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    TcpClient tcpClient =
        TcpClient.create(ConnectionProvider.elastic("tcp-client"))
            .runOn(LoopResources.create("client", 1, true))
            .host(Configurations.MDC_ADDRESS)
            .port(Configurations.MDC_PORT);

    RSocket client =
        RSocketFactory.connect()
            .frameDecoder(Frame::retain)
            .transport(() -> TcpClientTransport.create(tcpClient))
            .start()
            .block();

    Disposable report = startReport();

    client
        .requestChannel(
            Mono.fromCallable(System::nanoTime)
//                .delayElement(Duration.ofSeconds(1))
                .repeat((int) Configurations.NUMBER_OF_MESSAGES)
                .map(
                    start -> {
                      ByteBuf buffer = ByteBufAllocator.DEFAULT
                          .buffer(Configurations.MESSAGE_LENGTH);
                      return ByteBufPayload.create(buffer.writeLong(start));
                    }).subscribeOn(Schedulers.single()).publishOn(Schedulers.single())
        )
//        .log("dasd ")
        .doOnNext(Payload::release)
        .doOnNext(
            payload -> {
              long diff = System.nanoTime() - payload.getData().getLong(0);
              HISTOGRAM.recordValue(diff);
            })
        .doOnError(Throwable::printStackTrace)
        .doOnTerminate(
            () -> System.out.println("Sent " + Configurations.NUMBER_OF_MESSAGES + " messages."))
        .doFinally(s -> report.dispose())
        .then()
        .block();
  }

  private static Disposable startReport() {
    return Flux.interval(
            Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
            Duration.ofSeconds(Configurations.REPORT_INTERVAL))
        .doOnNext(RsocketTcpPing2::report)
        .doFinally(RsocketTcpPing2::report)
        .subscribe();
  }

  private static void report(Object ignored) {
    System.out.println("---- PING/PONG HISTO ----");
    HISTOGRAM.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
    System.out.println("---- PING/PONG HISTO ----");
  }
}

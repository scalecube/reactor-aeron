package rsocket.demo;

import io.aeron.driver.Configuration;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.console.ContinueBarrier;
import reactor.aeron.demo.Configurations;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public final class TcpPing {

  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);

  private static final ByteBuffer BUFFER =
      BufferUtil.allocateDirectAligned(Configurations.MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH);

  public static void main(String... args) {
    TcpClient tcpClient = TcpClient.create(ConnectionProvider.elastic("tcp-client"))
        .runOn(LoopResources.create("client", 1, true))
        .host("localhost")
        .port(7878);


    RSocket client =
        RSocketFactory.connect()
            .frameDecoder(Frame::retain)
            .transport(() -> TcpClientTransport.create(tcpClient))
            .start()
            .block();

    System.out.println(
        "address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT
            + ", controlPort: "
            + Configurations.MDC_CONTROL_PORT);
    System.out.println("MediaDriver THREADING_MODE: " + Configuration.THREADING_MODE_DEFAULT);
    System.out.println("Message length of " + Configurations.MESSAGE_LENGTH + " bytes");
    System.out.println("pollFragmentLimit of " + Configurations.FRAGMENT_COUNT_LIMIT);
    System.out.println(
        "Using worker idle strategy "
            + Configurations.idleStrategy().getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");
    System.out.println("Request " + Configurations.REQUESTED);

    ContinueBarrier barrier = new ContinueBarrier("Execute again?");
    do {
      System.out.println("Pinging " + Configurations.NUMBER_OF_MESSAGES + " messages");
      roundTripMessages(client, Configurations.NUMBER_OF_MESSAGES);
      System.out.println("Histogram of RTT latencies in microseconds.");
    } while (barrier.await());

    client.dispose();

    client.onClose().block();
  }

  private static void roundTripMessages(RSocket connection, long count) {
    HISTOGRAM.reset();

    Disposable reporter = startReport();

    connection
        .requestChannel(

            Flux.range(0, 1000000).map(TcpPing::payload)

//            Flux.range(0, Byte.MAX_VALUE)
//                .repeat()
//                .map(TcpPing::payload)
//                .take(Configurations.NUMBER_OF_MESSAGES).log("afs ")

            //                .limitRequest(Configurations.REQUESTED)
            )
        .doOnNext(
            payload -> {
              long start = payload.getData().getLong();
              System.out.println(start);
              long diff = System.nanoTime() - start;
              HISTOGRAM.recordValue(diff);
              payload.release();
            })
        .then(
            Mono.defer(
                () ->
                    Mono.delay(Duration.ofMillis(100))
                        .doOnSubscribe(s -> reporter.dispose())
                        .then()))
        .block();
  }

  private static Payload payload(Object ignore) {
//    BUFFER.putLong(0, System.nanoTime());
//    BUFFER.rewind();

//    return DefaultPayload.create(BUFFER);

    ByteBuffer buffer = ByteBuffer.wrap(new byte[Configurations.MESSAGE_LENGTH]);
    ByteBuffer byteBuffer = buffer.putLong(0, System.nanoTime());
    byteBuffer.rewind();


    return DefaultPayload.create(byteBuffer);
  }

  private static Disposable startReport() {
//    return Flux.interval(Duration.ofSeconds(5), Duration.ofSeconds(Configurations.REPORT_INTERVAL))
    return Flux.interval(Duration.ofSeconds(1))
        .doOnNext(TcpPing::report)
        .doFinally(TcpPing::report)
        .subscribe();
  }

  private static void report(Object ignored) {
    System.out.println("---- PING/PONG HISTO ----");
    HISTOGRAM.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
    System.out.println("---- PING/PONG HISTO ----");
  }
}

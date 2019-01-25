package reactor.aeron.demo;

import java.time.Duration;
import org.HdrHistogram.Recorder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronConnection;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class AeronPingClientSingleMediaDriver {

  private static final Duration REPORT_INTERVAL = Duration.ofSeconds(1);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    AeronResources resources =
        new AeronResources()
            .pollFragmentLimit(8)
            .aeron(ctx -> ctx.aeronDirectoryName("/tmp/aeron-SingleMediaDriver"))
            .workerIdleStrategySupplier(BusySpinIdleStrategy::new)
            .singleWorker()
            .start()
            .block();

    AeronConnection connection =
        AeronClient.create(resources).options("localhost", 13000, 13001).connect().block();

    // start reporter
    Recorder histogram = new Recorder(3600000000000L, 3);
    Flux.interval(REPORT_INTERVAL)
        .doOnNext(
            i -> {
              System.out.println("---- PING/PONG HISTO ----");
              histogram
                  .getIntervalHistogram()
                  .outputPercentileDistribution(System.out, 5, 1000.0, false);
              System.out.println("---- PING/PONG HISTO ----");
            })
        .subscribe();

    // send current nano time as body
    connection
        .outbound()
        .send(
            Mono.fromCallable(AeronPingClientSingleMediaDriver::generatePayload)
                .repeat(Integer.MAX_VALUE))
        .then()
        .doOnTerminate(connection::dispose)
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    // receive back sent start time and update the histogram
    connection
        .inbound()
        .receive()
        .doOnNext(
            buffer -> {
              long start = buffer.getLong(0);
              long diff = System.nanoTime() - start;
              histogram.recordValue(diff);
            })
        .doOnError(Throwable::printStackTrace)
        .subscribe();

    connection.onDispose(resources).onDispose().block();
  }

  private static DirectBuffer generatePayload() {
    ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(Long.BYTES);
    buffer.putLong(0, System.nanoTime());
    return buffer;
  }
}

package reactor.aeron;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class AeronPingClient {

  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);
  private static final LatencyReporter reporter = new LatencyReporter(HISTOGRAM);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(Configurations.FRAGMENT_COUNT_LIMIT)
            .singleWorker()
            .workerIdleStrategySupplier(Configurations::idleStrategy)
            .start()
            .block();

    AeronConnection connection =
        AeronClient.create(resources)
            .options(
                Configurations.MDC_ADDRESS,
                Configurations.MDC_PORT,
                Configurations.MDC_CONTROL_PORT)
            .connect()
            .block();

    System.out.println(
        "address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT
            + ", controlPort: "
            + Configurations.MDC_CONTROL_PORT);
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
      roundTripMessages(connection, Configurations.NUMBER_OF_MESSAGES);
      System.out.println("Histogram of RTT latencies in microseconds.");
    } while (barrier.await());

    connection.dispose();

    connection.onDispose(resources).onDispose().block();
  }

  private static void roundTripMessages(AeronConnection connection, long count) {
    HISTOGRAM.reset();

    Disposable disp = reporter.start();

    NanoTimeGeneratorHandler handler = new NanoTimeGeneratorHandler();

    connection.outbound().send(Flux.range(0, Configurations.REQUESTED), handler).then().subscribe();

    connection
        .outbound()
        .send(
            connection
                .inbound()
                .receive()
                .take(count)
                .doOnNext(
                    buffer -> {
                      long start = buffer.getLong(0);
                      long diff = System.nanoTime() - start;
                      HISTOGRAM.recordValue(diff);
                    }),
            handler)
        .then(
            Mono.defer(
                () -> Mono.delay(Duration.ofMillis(100)).doOnSubscribe(s -> disp.dispose()).then()))
        .then()
        .block();
  }

  private static class NanoTimeGeneratorHandler implements DirectBufferHandler<Object> {
    private static final UnsafeBuffer OFFER_BUFFER =
        new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(
                Configurations.MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));

    @Override
    public int estimateLength(Object ignore) {
      return Configurations.MESSAGE_LENGTH;
    }

    @Override
    public DirectBuffer map(Object ignore, int length) {
      OFFER_BUFFER.putLong(0, System.nanoTime());
      return OFFER_BUFFER;
    }

    @Override
    public void dispose(Object ignore) {}
  }
}

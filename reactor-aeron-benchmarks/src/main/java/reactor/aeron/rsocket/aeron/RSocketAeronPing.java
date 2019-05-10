package reactor.aeron.rsocket.aeron;

import io.aeron.driver.Configuration;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.frame.decoder.ZeroCopyPayloadDecoder;
import io.rsocket.reactor.aeron.AeronClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.aeron.Configurations;
import reactor.aeron.LatencyReporter;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public final class RSocketAeronPing {

  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);
  private static final LatencyReporter latencyReporter = new LatencyReporter(HISTOGRAM);

  private static final Payload PAYLOAD =
      ByteBufPayload.create(ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH));

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    printSettings();

    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(Configurations.FRAGMENT_COUNT_LIMIT)
            .singleWorker()
            .workerIdleStrategySupplier(Configurations::idleStrategy)
            .start()
            .block();

    RSocket client =
        RSocketFactory.connect()
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(
                () ->
                    new AeronClientTransport(
                        AeronClient.create(resources)
                            .options(
                                Configurations.MDC_ADDRESS,
                                Configurations.MDC_PORT,
                                Configurations.MDC_CONTROL_PORT)))
            .start()
            .block();

    Disposable report = latencyReporter.start();

    Flux.range(1, (int) Configurations.NUMBER_OF_MESSAGES)
        .flatMap(
            i -> {
              long start = System.nanoTime();
              return client
                  .requestResponse(PAYLOAD.retain())
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
            () -> System.out.println("Sent " + Configurations.NUMBER_OF_MESSAGES + " messages."))
        .doFinally(s -> report.dispose())
        .then()
        .doFinally(s -> resources.dispose())
        .then(resources.onDispose())
        .block();
  }

  private static void printSettings() {
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
  }
}

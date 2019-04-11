package reactor.aeron;

import io.scalecube.trace.TraceReporter;
import io.scalecube.trace.jsonbin.JsonbinResponse;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class LatencyReporter {

  private static final TraceReporter reporter = new TraceReporter();

  private final Recorder histogram;
  private final String name;

  public LatencyReporter(Recorder histogram) {
    this(histogram, Configurations.REPORT_NAME);
  }

  public LatencyReporter(Recorder histogram, String name) {
    this.histogram = histogram;
    this.name = name;
  }

  /**
   * Starts the reporter.
   *
   * @return disposable result.
   */
  public Disposable start() {
    if (reporter.isActive()) {
      return Disposables.composite(
          Flux.interval(
                  Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
                  Duration.ofSeconds(Configurations.TRACE_REPORTER_INTERVAL))
              .publishOn(Schedulers.single())
              .flatMap(
                  i ->
                      reporter
                          .sendToJsonbin()
                          .filter(JsonbinResponse::success)
                          .flatMap(
                              res ->
                                  reporter.dumpToFile(
                                      Configurations.TARGET_FOLDER_FOLDER_LATENCY,
                                      res.name(),
                                      res)))
              .subscribe(),
          Flux.interval(
                  Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
                  Duration.ofSeconds(Configurations.REPORT_INTERVAL))
              .publishOn(Schedulers.single())
              .doOnNext(
                  i ->
                      reporter.addY(this.name, histogram.getIntervalHistogram().getMean() / 1000.0))
              .subscribe());
    }
    return Flux.interval(
            Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
            Duration.ofSeconds(Configurations.REPORT_INTERVAL))
        .publishOn(Schedulers.single())
        .doOnNext(
            i -> {
              System.out.println("---- PING/PONG HISTO ----");
              histogram
                  .getIntervalHistogram()
                  .outputPercentileDistribution(System.out, 5, 1000.0, false);
              System.out.println("---- PING/PONG HISTO ----");
            })
        .subscribe();
  }
}

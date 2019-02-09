package reactor.aeron.demo;

import io.aeron.Image;
import io.aeron.Subscription;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

/** Configuration used for samples with defaults which can be overridden by system properties. */
public interface Configurations {
  int FRAGMENT_COUNT_LIMIT = Integer.getInteger("reactor.aeron.sample.frameCountLimit", 10);
  int MESSAGE_LENGTH = Integer.getInteger("reactor.aeron.sample.messageLength", 32);
  int WARMUP_NUMBER_OF_ITERATIONS = Integer.getInteger("reactor.aeron.sample.warmup.iterations", 5);
  long WARMUP_NUMBER_OF_MESSAGES = Long.getLong("reactor.aeron.sample.warmup.messages", 10_000);
  long NUMBER_OF_MESSAGES = Long.getLong("reactor.aeron.sample.messages", 10_000_000);
  boolean EXCLUSIVE_PUBLICATIONS =
      Boolean.getBoolean("reactor.aeron.sample.exclusive.publications");
  boolean EMBEDDED_MEDIA_DRIVER = Boolean.getBoolean("reactor.aeron.sample.embeddedMediaDriver");
  boolean INFO_FLAG = Boolean.getBoolean("reactor.aeron.sample.info");
  int PING_STREAM_ID = Integer.getInteger("reactor.aeron.sample.ping.streamId", 10);
  int PONG_STREAM_ID = Integer.getInteger("reactor.aeron.sample.pong.streamId", 10);
  String PING_CHANNEL =
      System.getProperty("reactor.aeron.sample.ping.channel", "aeron:udp?endpoint=localhost:40123");
  String PONG_CHANNEL =
      System.getProperty("reactor.aeron.sample.pong.channel", "aeron:udp?endpoint=localhost:40124");
  String MDC_ADDRESS = System.getProperty("reactor.aeron.sample.mdc.address", "localhost");
  int MDC_PORT = Integer.getInteger("reactor.aeron.sample.mdc.port", 13000);
  int MDC_CONTROL_PORT = Integer.getInteger("reactor.aeron.sample.mdc.control.port", 13001);
  int MDC_STREAM_ID = Integer.getInteger("reactor.aeron.sample.mdc.stream.id", 0xcafe0000);
  int MDC_SESSION_ID = Integer.getInteger("reactor.aeron.sample.mdc.session.id", 1001);
  int REQUESTED = Integer.getInteger("reactor.aeron.sample.request", 8);
  String IDLE_STRATEGY = System.getProperty("reactor.aeron.sample.idle.strategy", "busyspin");
  long REPORT_INTERVAL = Long.getLong("reactor.aeron.sample.report.interval", 1);
  long WARMUP_REPORT_DELAY = Long.getLong("reactor.aeron.sample.report.delay", REPORT_INTERVAL);

  /**
   * Returns idle strategy.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@link BackoffIdleStrategy} - backoff/1/1/1/100
   *   <li>{@link BusySpinIdleStrategy} - busyspin
   *   <li>{@link SleepingIdleStrategy} - sleeping/100
   *   <li>{@link SleepingMillisIdleStrategy} - sleepingmillis/1
   *   <li>{@link YieldingIdleStrategy} - yielding
   *   <li>{@link NoOpIdleStrategy} - noop
   * </ul>
   *
   * @return idle strategy, {@link BusySpinIdleStrategy} - by default
   */
  static IdleStrategy idleStrategy() {
    String[] chunks = IDLE_STRATEGY.split("/");
    switch (chunks[0].toLowerCase()) {
      case "backoff":
        return new BackoffIdleStrategy(
            Long.parseLong(chunks[1]),
            Long.parseLong(chunks[2]),
            Long.parseLong(chunks[3]),
            Long.parseLong(chunks[4]));
      case "busyspin":
        return new BusySpinIdleStrategy();
      case "sleeping":
        return new SleepingIdleStrategy(Long.parseLong(chunks[1]));
      case "sleepingmillis":
        return new SleepingMillisIdleStrategy(Long.parseLong(chunks[1]));
      case "yielding":
        return new YieldingIdleStrategy();
      case "noop":
        return new NoOpIdleStrategy();
      default:
        return new BusySpinIdleStrategy();
    }
  }

  /**
   * Print the information for an available image to stdout.
   *
   * @param image that has been created
   */
  static void printAvailableImage(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.println(
        String.format(
            "Available image on %s streamId=%d sessionId=%d from %s",
            subscription.channel(),
            subscription.streamId(),
            image.sessionId(),
            image.sourceIdentity()));
  }

  /**
   * Print the information for an unavailable image to stdout.
   *
   * @param image that has gone inactive
   */
  static void printUnavailableImage(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.println(
        String.format(
            "Unavailable image on %s streamId=%d sessionId=%d",
            subscription.channel(), subscription.streamId(), image.sessionId()));
  }
}

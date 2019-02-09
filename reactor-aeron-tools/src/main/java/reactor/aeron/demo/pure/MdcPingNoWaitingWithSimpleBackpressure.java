package reactor.aeron.demo.pure;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;
import reactor.aeron.demo.Configurations;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Ping component of Ping-Pong latency test recorded to a histogram to capture full distribution..
 *
 * <p>Initiates messages sent to {@link MdcPong} and records times.
 *
 * @see MdcPong
 */
public class MdcPingNoWaitingWithSimpleBackpressure {
  private static final int STREAM_ID = Configurations.MDC_STREAM_ID;
  private static final int PORT = Configurations.MDC_PORT;
  private static final int CONTROL_PORT = Configurations.MDC_CONTROL_PORT;
  private static final int SESSION_ID = Configurations.MDC_SESSION_ID;
  private static final String OUTBOUND_CHANNEL =
      new ChannelUriStringBuilder()
          .endpoint(Configurations.MDC_ADDRESS + ':' + PORT)
          .sessionId(SESSION_ID)
          .media("udp")
          .reliable(Boolean.TRUE)
          .build();
  private static final String INBOUND_CHANNEL =
      new ChannelUriStringBuilder()
          .controlEndpoint(Configurations.MDC_ADDRESS + ':' + CONTROL_PORT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .sessionId(SESSION_ID ^ Integer.MAX_VALUE)
          .reliable(Boolean.TRUE)
          .media("udp")
          .build();

  private static final long NUMBER_OF_MESSAGES = Configurations.NUMBER_OF_MESSAGES;
  private static final long WARMUP_NUMBER_OF_MESSAGES = Configurations.WARMUP_NUMBER_OF_MESSAGES;
  private static final int WARMUP_NUMBER_OF_ITERATIONS = Configurations.WARMUP_NUMBER_OF_ITERATIONS;
  private static final int MESSAGE_LENGTH = Configurations.MESSAGE_LENGTH;
  private static final int FRAGMENT_COUNT_LIMIT = Configurations.FRAGMENT_COUNT_LIMIT;
  private static final boolean EMBEDDED_MEDIA_DRIVER = Configurations.EMBEDDED_MEDIA_DRIVER;
  private static final boolean EXCLUSIVE_PUBLICATIONS = Configurations.EXCLUSIVE_PUBLICATIONS;
  private static final int REQUESTED = Configurations.REQUESTED;

  private static final UnsafeBuffer OFFER_BUFFER =
      new UnsafeBuffer(BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
  private static final Recorder HISTOGRAM = new Recorder(TimeUnit.SECONDS.toNanos(10), 3);
  private static final CountDownLatch LATCH = new CountDownLatch(1);
  private static final IdleStrategy POLLING_IDLE_STRATEGY = Configurations.idleStrategy();

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(final String[] args) throws Exception {
    final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;
    final Aeron.Context ctx =
        new Aeron.Context()
            .availableImageHandler(
                MdcPingNoWaitingWithSimpleBackpressure::availablePongImageHandler);
    final FragmentHandler fragmentHandler =
        new FragmentAssembler(MdcPingNoWaitingWithSimpleBackpressure::pongHandler);

    if (EMBEDDED_MEDIA_DRIVER) {
      ctx.aeronDirectoryName(driver.aeronDirectoryName());
    }

    System.out.println("MediaDriver THREADING_MODE: " + Configuration.THREADING_MODE_DEFAULT);
    System.out.println("Publishing Ping at " + OUTBOUND_CHANNEL + " on stream Id " + STREAM_ID);
    System.out.println("Subscribing Pong at " + INBOUND_CHANNEL + " on stream Id " + STREAM_ID);
    System.out.println("Message length of " + MESSAGE_LENGTH + " bytes");
    System.out.println("Using exclusive publications " + EXCLUSIVE_PUBLICATIONS);
    System.out.println(
        "Using poling idle strategy "
            + POLLING_IDLE_STRATEGY.getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");
    System.out.println("Request " + REQUESTED);

    try (Aeron aeron = Aeron.connect(ctx);
        Subscription subscription = aeron.addSubscription(INBOUND_CHANNEL, STREAM_ID);
        Publication publication =
            EXCLUSIVE_PUBLICATIONS
                ? aeron.addExclusivePublication(OUTBOUND_CHANNEL, STREAM_ID)
                : aeron.addPublication(OUTBOUND_CHANNEL, STREAM_ID)) {
      System.out.println("Waiting for new image from Pong...");
      LATCH.await();

      System.out.println(
          "Warming up... "
              + WARMUP_NUMBER_OF_ITERATIONS
              + " iterations of "
              + WARMUP_NUMBER_OF_MESSAGES
              + " messages");

      for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++) {
        roundTripMessages(
            fragmentHandler, publication, subscription, WARMUP_NUMBER_OF_MESSAGES, true);
      }

      Thread.sleep(100);
      final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

      do {
        System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");
        roundTripMessages(fragmentHandler, publication, subscription, NUMBER_OF_MESSAGES, false);
        System.out.println("Histogram of RTT latencies in microseconds.");
      } while (barrier.await());
    }

    CloseHelper.quietClose(driver);
  }

  private static void roundTripMessages(
      final FragmentHandler fragmentHandler,
      final Publication publication,
      final Subscription subscription,
      final long count,
      final boolean warmup) {
    while (!subscription.isConnected()) {
      Thread.yield();
    }

    HISTOGRAM.reset();

    Disposable reporter = startReport(warmup);

    final Image image = subscription.imageAtIndex(0);

    int produced = 0;
    int received = 0;

    for (long i = 0; i < count; ) {
      int workCount = 0;

      if (produced < REQUESTED) {
        OFFER_BUFFER.putLong(0, System.nanoTime());

        final long offeredPosition = publication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH);

        if (offeredPosition > 0) {
          i++;
          workCount = 1;
          produced++;
        }
      }

      final int poll = image.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);

      workCount += poll;
      received += poll;
      produced -= poll;

      POLLING_IDLE_STRATEGY.idle(workCount);
    }

    while (received < count) {
      final int poll = image.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
      received += poll;
      POLLING_IDLE_STRATEGY.idle(poll);
    }

    Mono.delay(Duration.ofMillis(100)).doOnSubscribe(s -> reporter.dispose()).then().subscribe();
  }

  private static void pongHandler(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {
    final long pingTimestamp = buffer.getLong(offset);
    final long rttNs = System.nanoTime() - pingTimestamp;

    HISTOGRAM.recordValue(rttNs);
  }

  private static void availablePongImageHandler(final Image image) {
    final Subscription subscription = image.subscription();
    System.out.format(
        "Available image: channel=%s streamId=%d session=%d%n",
        subscription.channel(), subscription.streamId(), image.sessionId());

    if (STREAM_ID == subscription.streamId() && INBOUND_CHANNEL.equals(subscription.channel())) {
      LATCH.countDown();
    }
  }

  private static Disposable startReport(boolean warmup) {
    if (warmup) {
      return () -> {
        // no-op
      };
    }
    return Flux.interval(
            Duration.ofSeconds(Configurations.WARMUP_REPORT_DELAY),
            Duration.ofSeconds(Configurations.REPORT_INTERVAL))
        .doOnNext(MdcPingNoWaitingWithSimpleBackpressure::report)
        .doFinally(MdcPingNoWaitingWithSimpleBackpressure::report)
        .subscribe();
  }

  private static void report(Object ignored) {
    System.out.println("---- PING/PONG HISTO ----");
    HISTOGRAM.getIntervalHistogram().outputPercentileDistribution(System.out, 5, 1000.0, false);
    System.out.println("---- PING/PONG HISTO ----");
  }
}

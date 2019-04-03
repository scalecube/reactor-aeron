package reactor.aeron.pure;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import java.util.concurrent.CountDownLatch;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.console.ContinueBarrier;
import reactor.aeron.Configurations;

public class ClientThroughput {
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
  private static final int MESSAGE_LENGTH = Configurations.MESSAGE_LENGTH;
  private static final boolean EMBEDDED_MEDIA_DRIVER = Configurations.EMBEDDED_MEDIA_DRIVER;
  private static final boolean EXCLUSIVE_PUBLICATIONS = Configurations.EXCLUSIVE_PUBLICATIONS;

  private static final UnsafeBuffer OFFER_BUFFER =
      new UnsafeBuffer(BufferUtil.allocateDirectAligned(MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));
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
        new Aeron.Context().availableImageHandler(ClientThroughput::availablePongImageHandler);

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

    try (Aeron aeron = Aeron.connect(ctx);
        Subscription subscription = aeron.addSubscription(INBOUND_CHANNEL, STREAM_ID);
        Publication publication =
            EXCLUSIVE_PUBLICATIONS
                ? aeron.addExclusivePublication(OUTBOUND_CHANNEL, STREAM_ID)
                : aeron.addPublication(OUTBOUND_CHANNEL, STREAM_ID)) {
      System.out.println("Waiting for new image from Pong...");
      LATCH.await();

      while (!subscription.isConnected()) {
        Thread.yield();
      }

      Thread.sleep(100);
      final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

      do {
        System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");

        for (long i = 0; i < Long.MAX_VALUE; ) {
          OFFER_BUFFER.putLong(0, System.nanoTime());
          final long offeredPosition = publication.offer(OFFER_BUFFER, 0, MESSAGE_LENGTH);
          if (offeredPosition > 0) {
            i++;
            continue;
          }
          POLLING_IDLE_STRATEGY.idle();
        }

        System.out.println("Histogram of RTT latencies in microseconds.");
      } while (barrier.await());
    }

    CloseHelper.quietClose(driver);
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
}

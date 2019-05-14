package reactor.aeron.pure;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;
import reactor.aeron.Configurations;
import reactor.aeron.RateReporter;

public class ServerThroughput {

  private static final int STREAM_ID = Configurations.MDC_STREAM_ID;
  private static final int PORT = Configurations.MDC_PORT;
  private static final int CONTROL_PORT = Configurations.MDC_CONTROL_PORT;
  private static final int SESSION_ID = Configurations.MDC_SESSION_ID;
  private static final String OUTBOUND_CHANNEL =
      new ChannelUriStringBuilder()
          .controlEndpoint(Configurations.MDC_ADDRESS + ':' + CONTROL_PORT)
          .sessionId(SESSION_ID ^ Integer.MAX_VALUE)
          .media("udp")
          .reliable(Boolean.TRUE)
          .build();
  private static final String INBOUND_CHANNEL =
      new ChannelUriStringBuilder()
          .endpoint(Configurations.MDC_ADDRESS + ':' + PORT)
          .sessionId(SESSION_ID)
          .reliable(Boolean.TRUE)
          .media("udp")
          .build();

  private static final int FRAME_COUNT_LIMIT = Configurations.FRAGMENT_COUNT_LIMIT;
  private static final boolean INFO_FLAG = Configurations.INFO_FLAG;
  private static final boolean EMBEDDED_MEDIA_DRIVER = Configurations.EMBEDDED_MEDIA_DRIVER;
  private static final boolean EXCLUSIVE_PUBLICATIONS = Configurations.EXCLUSIVE_PUBLICATIONS;

  private static final IdleStrategy PING_HANDLER_IDLE_STRATEGY = Configurations.idleStrategy();

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(final String[] args) {
    final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;

    final Aeron.Context ctx = new Aeron.Context();
    if (EMBEDDED_MEDIA_DRIVER) {
      ctx.aeronDirectoryName(driver.aeronDirectoryName());
    }

    if (INFO_FLAG) {
      ctx.availableImageHandler(Configurations::printAvailableImage);
      ctx.unavailableImageHandler(Configurations::printUnavailableImage);
    }

    final IdleStrategy idleStrategy = Configurations.idleStrategy();

    System.out.println("Subscribing Ping at " + INBOUND_CHANNEL + " on stream Id " + STREAM_ID);
    System.out.println("Publishing Pong at " + OUTBOUND_CHANNEL + " on stream Id " + STREAM_ID);
    System.out.println("Using exclusive publications " + EXCLUSIVE_PUBLICATIONS);
    System.out.println(
        "Using ping handler idle strategy "
            + PING_HANDLER_IDLE_STRATEGY.getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");

    final AtomicBoolean running = new AtomicBoolean(true);
    SigInt.register(() -> running.set(false));

    RateReporter reporter = new RateReporter();
    
    try (Aeron aeron = Aeron.connect(ctx);
        Subscription subscription = aeron.addSubscription(INBOUND_CHANNEL, STREAM_ID);
        Publication publication =
            EXCLUSIVE_PUBLICATIONS
                ? aeron.addExclusivePublication(OUTBOUND_CHANNEL, STREAM_ID)
                : aeron.addPublication(OUTBOUND_CHANNEL, STREAM_ID)) {

      final FragmentAssembler dataHandler =
          new FragmentAssembler((buffer, offset, length, header) -> reporter.onMessage(1, length));

      while (running.get()) {
        idleStrategy.idle(subscription.poll(dataHandler, FRAME_COUNT_LIMIT));
      }

      System.out.println("Shutting down...");
    } finally {
      reporter.dispose();
    }

    CloseHelper.quietClose(driver);
  }
}

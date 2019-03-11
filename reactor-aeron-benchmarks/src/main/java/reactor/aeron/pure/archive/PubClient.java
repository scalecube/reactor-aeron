package reactor.aeron.pure.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;

public class PubClient {

  private static final String BROKER_CONTROL_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .controlEndpoint(Broker.BROKER_REPLAY_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int BROKER_CONTROL_STREAM_ID = Broker.BROKER_REPLAY_STREAM_ID;

  private static final UnsafeBuffer BUFFER =
      new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));

  private static MediaDriver mediaDriver;
  private static Aeron aeron;

  public static void main(String[] args) throws InterruptedException {
    try {
      String aeronDirName = Utils.tmpFileName("aeron");

      mediaDriver =
          MediaDriver.launch(
              new Context()
                  .threadingMode(ThreadingMode.SHARED)
                  .errorHandler(Throwable::printStackTrace)
                  .aeronDirectoryName(aeronDirName)
                  .dirDeleteOnStart(true));

      aeron =
          Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

      final AtomicBoolean running = new AtomicBoolean(true);
      SigInt.register(() -> running.set(false));

      try (Publication publication =
          aeron.addExclusivePublication(BROKER_CONTROL_CHANNEL_URI, BROKER_CONTROL_STREAM_ID)) {

        int n = 100000;
        for (int i = 0; i < n && running.get(); i++) {
          final String message = "Hello World! " + i;
          final byte[] messageBytes = message.getBytes();
          BUFFER.putBytes(0, messageBytes);

          System.out.print("Offering " + i + "/" + n + " - ");

          final long result = publication.offer(BUFFER, 0, messageBytes.length);
          checkResult(result);

          Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }
      }

    } finally {
      if (aeron != null) {
        aeron.close();
      }
      if (mediaDriver != null) {
        mediaDriver.close();
      }

      if (aeron != null) {
        IoUtil.delete(aeron.context().aeronDirectory(), true);
      }
      if (mediaDriver != null) {
        IoUtil.delete(mediaDriver.context().aeronDirectory(), true);
      }
    }
  }

  private static void checkResult(final long result) {
    if (result > 0) {
      System.out.println("yay!");
    } else if (result == Publication.BACK_PRESSURED) {
      System.out.println("Offer failed due to back pressure");
    } else if (result == Publication.ADMIN_ACTION) {
      System.out.println("Offer failed because of an administration action in the system");
    } else if (result == Publication.NOT_CONNECTED) {
      System.out.println("Offer failed because publisher is not connected to subscriber");
    } else if (result == Publication.CLOSED) {
      System.out.println("Offer failed publication is closed");
    } else if (result == Publication.MAX_POSITION_EXCEEDED) {
      throw new IllegalStateException("Offer failed due to publication reaching max position");
    } else {
      System.out.println("Offer failed due to unknown result code: " + result);
    }
  }
}

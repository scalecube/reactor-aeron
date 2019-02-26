package reactor.aeron.archive;

import static reactor.aeron.archive.MessageBroker.MY_CHANNEL;
import static reactor.aeron.archive.MessageBroker.MY_STREAM_ID;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Flux;

public class Client2 {

  private static final UnsafeBuffer BUFFER =
      new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));

  private static MediaDriver mediaDriver;
  private static Aeron aeron;
  private static AeronArchive aeronArchive;

  public static void main(String[] args) throws InterruptedException {

    try {

      String aeronDirName = tmpFileName("aeron");

      mediaDriver =
          MediaDriver.launch(
              new Context()
                  .threadingMode(ThreadingMode.SHARED)
                  .spiesSimulateConnection(false)
                  .errorHandler(Throwable::printStackTrace)
                  .aeronDirectoryName(aeronDirName)
                  .dirDeleteOnStart(true));

      aeron =
          Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));





      aeronArchive =
          AeronArchive.connect(
              new AeronArchive.Context()
                  .controlResponseChannel("aeron:udp?endpoint=localhost:8021")
                  .aeron(aeron)
                  .ownsAeronClient(true));

      String udp = new ChannelUriStringBuilder()
          .controlEndpoint("localhost" + ':' + 54327)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
//          .sessionId(SESSION_ID ^ Integer.MAX_VALUE)
          .reliable(Boolean.TRUE)
          .media("udp")
          .build();

      try (Subscription subscription =
          aeron
              .addSubscription(udp, 2222)) {

        Flux.interval(Duration.ofMillis(100))
            .doOnNext(
                i ->
                    subscription.poll(
                        (buffer, offset, length, header) -> {
                          System.out.println(length);
                        },
                        10))
            .blockLast();


      }

    } finally {
      if (aeronArchive != null) {
        aeronArchive.close();
      }
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

  private static String tmpFileName(String value) {
    return IoUtil.tmpDirName()
        + value
        + '-'
        + System.getProperty("user.name", "default")
        + '-'
        + UUID.randomUUID().toString();
  }

}

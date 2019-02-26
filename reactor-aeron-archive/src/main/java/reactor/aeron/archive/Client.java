package reactor.aeron.archive;

import static reactor.aeron.archive.MessageBroker.MY_CHANNEL;
import static reactor.aeron.archive.MessageBroker.MY_STREAM_ID;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import reactor.core.publisher.Flux;

public class Client {

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

      ExclusivePublication exclusivePublication = aeron
          .addExclusivePublication(MY_CHANNEL, MY_STREAM_ID);



      Flux.interval(Duration.ofMillis(100)).subscribe(i -> {

        final String message = "Hello World! " + i;
        final byte[] messageBytes = message.getBytes();
        BUFFER.putBytes(0, messageBytes);

        long workCount = exclusivePublication.offer(BUFFER);


      });



      Thread.currentThread().join();


      //        .controlResponseChannel("aeron:udp?endpoint=localhost:54327")
      //        .controlResponseStreamId(1001)
      aeronArchive =
          AeronArchive.connect(
              new AeronArchive.Context()
                  .aeron(aeron)
                  //        .controlResponseChannel("aeron:udp?endpoint=localhost:54327")
                  //        .controlResponseStreamId(1001)
                  .ownsAeronClient(true));




      long subscriptionId =
          aeronArchive.startRecording(
              MY_CHANNEL, MY_STREAM_ID, SourceLocation.REMOTE);

      final AtomicBoolean running = new AtomicBoolean(true);
      SigInt.register(() -> running.set(false));

      try (Publication publication =
          aeron
              .addExclusivePublication(MY_CHANNEL, MY_STREAM_ID)) {




        // Wait for recording to have started before publishing.
        final CountersReader counters = aeronArchive.context().aeron().countersReader();
        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
        while (CountersReader.NULL_COUNTER_ID == counterId) {


          if (!running.get())
          {
            return;
          }

          Thread.yield();
          counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());

        }

        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        System.out.println("Recording started: recordingId = " + recordingId);

        int n = 10;
        for (int i = 0; i < n && running.get(); i++) {
          final String message = "Hello World! " + i;
          final byte[] messageBytes = message.getBytes();
          BUFFER.putBytes(0, messageBytes);

          System.out.print("Offering " + i + "/" + n + " - ");

          final long result = publication.offer(BUFFER, 0, messageBytes.length);
          checkResult(result);

          final String errorMessage = aeronArchive.pollForErrorResponse();
          if (null != errorMessage) {
            throw new IllegalStateException(errorMessage);
          }

          Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }

        while (counters.getCounterValue(counterId) < publication.position()) {
          if (!RecordingPos.isActive(counters, counterId, recordingId)) {
            throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
          }

          Thread.yield();
        }
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

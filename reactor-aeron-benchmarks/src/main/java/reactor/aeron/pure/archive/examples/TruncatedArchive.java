package reactor.aeron.pure.archive.examples;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TruncatedArchive {

  static final String OUTGOING_ENDPOINT = "localhost:8181";
  static final int OUTGOING_STREAM_ID = 2223;

  static final String OUTGOING_URI =
      new ChannelUriStringBuilder()
          .controlEndpoint(OUTGOING_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int MESSAGE_SIZE = 256;
  private static final UnsafeBuffer BUFFER =
      new UnsafeBuffer(BufferUtil.allocateDirectAligned(MESSAGE_SIZE, 64));

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    String aeronDirName = Utils.tmpFileName("aeron");
    String archiveDirName = aeronDirName + "-archive";

    try (ArchivingMediaDriver archivingMediaDriver =
            ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .spiesSimulateConnection(true)
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true),
                new Archive.Context()
                    .aeronDirectoryName(aeronDirName)
                    .archiveDirectoryName(archiveDirName)
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .errorHandler(Throwable::printStackTrace)
                    .fileSyncLevel(0)
                    .deleteArchiveOnStart(true));
        AeronArchive aeronArchive =
            AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(aeronDirName))) {
      print(archivingMediaDriver);

      Aeron aeron = aeronArchive.context().aeron();

      ExclusivePublication outgoingPublication =
          aeron.addExclusivePublication(OUTGOING_URI, OUTGOING_STREAM_ID);
      aeronArchive.startRecording(OUTGOING_URI, OUTGOING_STREAM_ID, SourceLocation.LOCAL);

      long recordingId =
          Utils.findRecording(aeronArchive, OUTGOING_URI, OUTGOING_STREAM_ID, 0, 100)
              .log("found recordings ")
              .blockLast()
              .recordingId;

      Flux.interval(Duration.ofSeconds(1))
          .doOnNext(
              i -> {
                final String message = "hello@" + i;
                final byte[] messageBytes = message.getBytes();
                BUFFER.putBytes(0, messageBytes);

                final long result = outgoingPublication.offer(BUFFER, 0, messageBytes.length);

                System.out.print(
                    "Sent " + i + ", current position: " + outgoingPublication.position() + " - ");
                checkResult(result);
              })
          .subscribe();

      Mono.delay(Duration.ofSeconds(10))
          .doOnSuccess(
              $ -> {
                aeronArchive.stopRecording(OUTGOING_URI, OUTGOING_STREAM_ID);

                long recordingPosition = aeronArchive.getRecordingPosition(recordingId);
                long stopPosition = aeronArchive.getStopPosition(recordingId);
                //                long truncatedPosition = stopPosition / 2;
                long truncatedPosition = 64;

                System.out.println("recordingPosition = " + recordingPosition);
                System.out.println("stopPosition = " + stopPosition);
                System.out.println("truncatedPosition = " + truncatedPosition);

                aeronArchive.truncateRecording(recordingId, truncatedPosition);

                try {
                  Thread.sleep(2000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }

                aeronArchive.startRecording(OUTGOING_URI, OUTGOING_STREAM_ID, SourceLocation.LOCAL);
              })
          .subscribe();

      Thread.currentThread().join();
    } finally {
      Utils.removeFile(archiveDirName);
      Utils.removeFile(aeronDirName);
    }
  }

  private static void print(ArchivingMediaDriver archivingMediaDriver) {
    MediaDriver mediaDriver = archivingMediaDriver.mediaDriver();
    Archive archive = archivingMediaDriver.archive();
    Archive.Context context = archivingMediaDriver.archive().context();

    System.out.println("Archive threadingMode: " + context.threadingMode());
    System.out.println("Archive controlChannel: " + context.controlChannel());
    System.out.println("Archive controlStreamId: " + context.controlStreamId());
    System.out.println("Archive localControlChannel: " + context.localControlChannel());
    System.out.println("Archive localControlStreamId: " + context.localControlStreamId());
    System.out.println("Archive recordingEventsChannel: " + context.recordingEventsChannel());
    System.out.println("Archive recordingEventsStreamId: " + context.recordingEventsStreamId());
    System.out.println("Archive controlTermBufferSparse: " + context.controlTermBufferSparse());
    System.out.println("Archive archiveDirName: " + archive.context().archiveDirectoryName());
    System.out.println("Archive aeronDirectoryName: " + mediaDriver.aeronDirectoryName());
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

package reactor.aeron.pure.archive.examples;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.BufferUtil;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.Configurations;
import reactor.aeron.pure.archive.Utils;

public class MatchEngineEventHandler {

  private static final String INCOMING_URI =
      new ChannelUriStringBuilder()
          .controlEndpoint(MatchEngine.OUTGOING_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int FRAGMENT_LIMIT = 10;

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    final AtomicBoolean running = new AtomicBoolean(true);
    SigInt.register(() -> running.set(false));

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

      aeronArchive.startRecording(INCOMING_URI, INCOMING_STREAM_ID, SourceLocation.LOCAL);
      aeronArchive.startRecording(OUTGOING_URI, OUTGOING_STREAM_ID, SourceLocation.LOCAL);

      Subscription subscription =
          aeron.addSubscription(
              INCOMING_URI,
              INCOMING_STREAM_ID,
              Configurations::printAvailableImage,
              Configurations::printUnavailableImage);

      ExclusivePublication publication =
          aeron.addExclusivePublication(OUTGOING_URI, OUTGOING_STREAM_ID);

      while (running.get()) {

        subscription.poll(
            (buffer, offset, length, header) -> {

              // some business logic

              if (length + Long.BYTES > MESSAGE_SIZE) {
                System.err.println("can't publish msg with its position");
                return;
              }

              BUFFER.putLong(0, header.position());
              BUFFER.putBytes(Long.BYTES, buffer, offset, length);
              publication.offer(BUFFER);
            },
            FRAGMENT_LIMIT);

        Thread.sleep(100);
      }

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

    System.out.println("Archive listen: " + INCOMING_URI);
  }
}

package reactor.aeron.pure.archive.examples.recording.simple;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;

public class RecordingServer {

  static final String INCOMING_RECORDING_ENDPOINT = "localhost:7373";
  static final int INCOMING_RECORDING_STREAM_ID = 3333;

  private static final String INCOMING_RECORDING_URI =
      new ChannelUriStringBuilder()
          .endpoint(INCOMING_RECORDING_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
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

      // it's important to bind necessary port
      aeronArchive
          .context()
          .aeron()
          .addSubscription(INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID);

      aeronArchive.startRecording(
          INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID, SourceLocation.REMOTE);

      Flux.interval(Duration.ofSeconds(5))
          .flatMap(
              i ->
                  Utils.findRecording(
                      aeronArchive, INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID, 0, 1000))
          .distinct(recordingDescriptor -> recordingDescriptor.recordingId)
          .log("fondRecording ")
          .blockLast();
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

    System.out.println(
        "Archive listen: "
            + INCOMING_RECORDING_ENDPOINT
            + ", streamId: "
            + INCOMING_RECORDING_STREAM_ID);
  }
}

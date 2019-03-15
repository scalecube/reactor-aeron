package reactor.aeron.pure.archive.examples;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.SigInt;
import reactor.aeron.Configurations;
import reactor.aeron.pure.archive.Utils;

public class IntermediateArchive {

  static final String CONTROL_ENDPOINT = "localhost:7171";
  static final int CONTROL_STREAM_ID = 2222;
  static final String REPLAY_ENDPOINT = "localhost:8181";
  static final int REPLAY_STREAM_ID = 2223;

  private static final ChannelUriStringBuilder CONTROL_URI_BUILDER =
      new ChannelUriStringBuilder()
          .endpoint(CONTROL_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA);

  private static final ChannelUriStringBuilder REPLAY_URI_BUILDER =
      new ChannelUriStringBuilder()
          .controlEndpoint(REPLAY_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA);

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
                    // .spiesSimulateConnection(true)
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

      aeron.addSubscription(
          REPLAY_URI_BUILDER.build(),
          REPLAY_STREAM_ID,
          Configurations::printAvailableImage,
          Configurations::printUnavailableImage);

      aeron.addSubscription(
          CONTROL_URI_BUILDER.build(),
          CONTROL_STREAM_ID,
          Configurations::printAvailableImage,
          Configurations::printUnavailableImage);

      String recordingChannel = CONTROL_URI_BUILDER.build();
      int recordingStreamId = CONTROL_STREAM_ID;

      long subscriptionId =
          aeronArchive.startRecording(recordingChannel, recordingStreamId, SourceLocation.REMOTE);

      System.out.println(
          "Created recording subscriptionId: "
              + subscriptionId
              + ", for channel: "
              + recordingChannel
              + ", streamId: "
              + recordingStreamId);

      HashSet<Long> recordingIds = new HashSet<>();

      while (running.get()) {

        aeronArchive.listRecordingsForUri(
            0,
            10000,
            recordingChannel,
            recordingStreamId,
            (controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) -> {
              if (recordingIds.add(recordingId)) {

                System.out.println("Found new recording id: " + recordingId);

                String replayChannel = REPLAY_URI_BUILDER.sessionId(sessionId).build();
                int replayStreamId = REPLAY_STREAM_ID;

                final long replaySessionId =
                    aeronArchive.startReplay(
                        recordingId, 0, Long.MAX_VALUE, replayChannel, replayStreamId);

                System.out.println(
                    "Started replaying, "
                        + "sessionId: "
                        + sessionId
                        + ", replaySessionId: "
                        + replaySessionId
                        + ", recordingId: "
                        + recordingId
                        + ", replay channel: "
                        + replayChannel
                        + ", streamId: "
                        + replayStreamId);
              }
            });

        Thread.sleep(1000);
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

    System.out.println("Archive listen: " + CONTROL_URI_BUILDER);
  }
}

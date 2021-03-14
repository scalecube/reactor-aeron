package reactor.aeron.pure.archive.examples;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import org.agrona.collections.MutableLong;
import reactor.aeron.Configurations;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;

public class SimpleBroker {

  static final String BROKER_CONTROL_ENDPOINT = "localhost:7171";
  static final int BROKER_CONTROL_STREAM_ID = 2222;
  static final String BROKER_REPLAY_ENDPOINT = "localhost:8181";
  static final int BROKER_REPLAY_STREAM_ID = 2223;

  private static final ChannelUriStringBuilder BROKER_CONTROL_URI_BUILDER =
      new ChannelUriStringBuilder()
          .endpoint(BROKER_CONTROL_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA);

  private static final ChannelUriStringBuilder BROKER_REPLAY_CHANNEL_URI_BUILDER =
      new ChannelUriStringBuilder()
          .controlEndpoint(BROKER_REPLAY_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA);

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
            AeronArchive.connect(
                new AeronArchive.Context() //
                    .aeronDirectoryName(aeronDirName)
                // .aeron(archivingMediaDriver.archive().context().aeron())
                // .controlResponseChannel("aeron:udp?endpoint=localhost:8021")
                // .controlResponseStreamId(18021)
                // .ownsAeronClient(true)
                ); ) {
      print(archivingMediaDriver);

      Aeron aeron = aeronArchive.context().aeron();

      aeron.addSubscription(
          BROKER_REPLAY_CHANNEL_URI_BUILDER.build(),
          BROKER_REPLAY_STREAM_ID,
          Configurations::printAvailableImage,
          Configurations::printUnavailableImage);

      Subscription subscription =
          aeron.addSubscription(
              BROKER_CONTROL_URI_BUILDER.build(),
              BROKER_CONTROL_STREAM_ID,
              image -> {
                Configurations.printAvailableImage(image);

                String recordingChannel =
                    new ChannelUriStringBuilder()
                        .endpoint(BROKER_CONTROL_ENDPOINT)
                        .sessionId(image.sessionId())
                        .reliable(Boolean.TRUE)
                        .media(CommonContext.UDP_MEDIA)
                        .build();

                long subscriptionId =
                    aeronArchive.startRecording(
                        recordingChannel, BROKER_CONTROL_STREAM_ID, SourceLocation.REMOTE);

                System.out.println(
                    "Created recording subscriptionId: "
                        + subscriptionId
                        + ", for channel: "
                        + recordingChannel
                        + ", streamId: "
                        + BROKER_CONTROL_STREAM_ID);

                long recordingId =
                    findLatestRecording(aeronArchive, recordingChannel, BROKER_CONTROL_STREAM_ID);

                System.out.println("Found recording id: " + recordingId);

                String replayChannel =
                    BROKER_REPLAY_CHANNEL_URI_BUILDER.sessionId(image.sessionId()).build();
                int replayStreamId = BROKER_REPLAY_STREAM_ID;

                final long sessionId =
                    aeronArchive.startReplay(
                        recordingId, 0, Long.MAX_VALUE, replayChannel, replayStreamId);

                System.out.println("Started replay session id: " + sessionId);

                System.out.println(
                    "Started replaying, recordingId: "
                        + recordingId
                        + ", replay channel: "
                        + replayChannel
                        + ", streamId: "
                        + replayStreamId);
              },
              Configurations::printUnavailableImage);

      Flux.interval(Duration.ofSeconds(5))
          .doOnNext(
              i -> {
                System.out.println("------------------- listRecordings -------------------");

                aeronArchive.listRecording(
                    0,
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
                      System.out.println(
                          new StringBuilder()
                              .append("controlSessionId: ")
                              .append(controlSessionId)
                              .append(", correlationId: ")
                              .append(correlationId)
                              .append(", recordingId: ")
                              .append(recordingId)
                              .append(", startTimestamp: ")
                              .append(startTimestamp)
                              .append(", stopTimestamp: ")
                              .append(stopTimestamp)
                              .append(", startPosition: ")
                              .append(startPosition)
                              .append(", stopPosition: ")
                              .append(stopPosition)
                              .append(", initialTermId: ")
                              .append(initialTermId)
                              .append(", segmentFileLength: ")
                              .append(segmentFileLength)
                              .append(", termBufferLength: ")
                              .append(termBufferLength)
                              .append(", mtuLength: ")
                              .append(mtuLength)
                              .append(", sessionId: ")
                              .append(sessionId)
                              .append(", streamId: ")
                              .append(streamId)
                              .append(", strippedChannel: ")
                              .append(strippedChannel)
                              .append(", originalChannel: ")
                              .append(originalChannel)
                              .append(", sourceIdentity: ")
                              .append(sourceIdentity));
                    });
                System.out.println("------------------------------------------------------");
              })
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

    System.out.println("Archive listen: " + BROKER_CONTROL_URI_BUILDER);
  }

  private static long findLatestRecording(
      final AeronArchive archive, String channel, int channelStreamId) {
    final MutableLong lastRecordingId = new MutableLong();

    final RecordingDescriptorConsumer consumer =
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
            sourceIdentity) -> lastRecordingId.set(recordingId);

    final long fromRecordingId = 0L;
    final int recordCount = 100;

    final int foundCount =
        archive.listRecordingsForUri(
            fromRecordingId, recordCount, channel, channelStreamId, consumer);

    if (foundCount == 0) {
      throw new IllegalStateException("no recordings found");
    }

    return lastRecordingId.get();
  }
}

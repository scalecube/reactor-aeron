package reactor.aeron.pure.archive.examples.recording.simple;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;

public class ExtendedOperation {

  private static final String CONTROL_RESPONSE_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .endpoint("localhost:8485")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int CONTROL_RESPONSE_STREAM_ID = 8485;

  private static final String TARGET_RECORDING_URI = RecordingServer.INCOMING_RECORDING_URI;
  private static final int TARGET_RECORDING_STREAM_ID =
      RecordingServer.INCOMING_RECORDING_STREAM_ID;

  private static final ChannelUriStringBuilder EXTENDED_RECORDING_URI_BUILDER =
      new ChannelUriStringBuilder()
          .endpoint("localhost:8486")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA);
  private static final int EXTENDED_RECORDING_STREAM_ID = 7777;

  private static final Duration SENT_INTERVAL = Duration.ofSeconds(1);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws InterruptedException {
    String aeronDirName = Utils.tmpFileName("aeron");

    try (MediaDriver mediaDriver =
            MediaDriver.launch(
                new Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .spiesSimulateConnection(true)
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true));
        AeronArchive aeronArchive =
            AeronArchive.connect(
                new AeronArchive.Context()
                    .controlResponseChannel(CONTROL_RESPONSE_CHANNEL_URI)
                    .controlResponseStreamId(CONTROL_RESPONSE_STREAM_ID)
                    .aeronDirectoryName(aeronDirName))) {

      Flux.interval(Duration.ofSeconds(5))
          .flatMap(
              i ->
                  Utils.findRecording(
                      aeronArchive, TARGET_RECORDING_URI, TARGET_RECORDING_STREAM_ID, 0, 1000))
          .distinct(recordingDescriptor -> recordingDescriptor.recordingId)
          .log("fondRecording ")
          .take(1)
          .doOnNext(
              recording -> {
                aeronArchive.stopRecording(recording.originalChannel, recording.streamId);

                aeronArchive.extendRecording(
                    recording.recordingId,
                    EXTENDED_RECORDING_URI_BUILDER.build(),
                    EXTENDED_RECORDING_STREAM_ID,
                    SourceLocation.REMOTE);

                long stopPosition = aeronArchive.getStopPosition(recording.recordingId);

                String channel =
                    EXTENDED_RECORDING_URI_BUILDER
                        .initialPosition(
                            stopPosition, recording.initialTermId, recording.termBufferLength)
                        .build();

                System.out.println(
                    "Creating publication to "
                        + channel
                        + "stream id: "
                        + EXTENDED_RECORDING_STREAM_ID);

                ExclusivePublication publication =
                    aeronArchive
                        .context()
                        .aeron()
                        .addExclusivePublication(channel, EXTENDED_RECORDING_STREAM_ID);

                Flux.interval(SENT_INTERVAL)
                    .map(i -> "extended msg " + i)
                    .doOnNext(message -> Utils.send(publication, message))
                    .subscribe();
              })
          .subscribe();

      Thread.currentThread().join();

    } finally {
      Utils.removeFile(aeronDirName);
    }
  }
}

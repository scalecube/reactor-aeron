package reactor.aeron.pure.archive.examples.recording.simple;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;

public class TruncatedOperation {

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

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
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
          .doOnNext(
              recording -> {
                aeronArchive.stopRecording(recording.originalChannel, recording.streamId);

                aeronArchive.truncateRecording(recording.recordingId, recording.startPosition);

                aeronArchive.startRecording(
                    recording.originalChannel, recording.streamId, SourceLocation.REMOTE);
              })
          .blockLast();
    } finally {
      Utils.removeFile(aeronDirName);
    }
  }
}

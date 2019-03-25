package reactor.aeron.pure.archive.examples.recording.simple;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.Configurations;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;

public class ReplayServer {

  private static final String CONTROL_RESPONSE_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .endpoint("localhost:8484")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int CONTROL_RESPONSE_STREAM_ID = 8484;

  private static final String TARGET_RECORDING_URI = RecordingServer.INCOMING_RECORDING_URI;
  private static final int TARGET_RECORDING_STREAM_ID =
      RecordingServer.INCOMING_RECORDING_STREAM_ID;

  private static final int REPLAY_STREAM_ID = 7474;
  private static final String REPLAY_URI =
      new ChannelUriStringBuilder()
          .endpoint("localhost:7474")
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int FRAGMENT_LIMIT = 1000;

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
          // check if the recording is ready for recording (truncated case)
          .filter(
              recording ->
                  recording.stopPosition == -1 || recording.startPosition < recording.stopPosition)
          .subscribe(
              recording ->
                  aeronArchive.startReplay(
                      recording.recordingId,
                      recording.startPosition,
                      AeronArchive.NULL_LENGTH,
                      REPLAY_URI,
                      REPLAY_STREAM_ID));

      Subscription subscription =
          aeronArchive
              .context()
              .aeron()
              .addSubscription(
                  REPLAY_URI,
                  REPLAY_STREAM_ID,
                  Configurations::printAvailableImage,
                  Configurations::printUnavailableImage);

      Flux.interval(Duration.ofSeconds(1))
          .doOnNext(
              i ->
                  subscription.poll(
                      (buffer, offset, length, header) -> {
                        final byte[] data = new byte[length];
                        buffer.getBytes(offset, data);

                        System.out.println(
                            String.format(
                                "Message from session %d (%d@%d) <<%s>>, header{ pos: %s, offset: %s, termOffset: %s, type: %s}",
                                header.sessionId(),
                                length,
                                offset,
                                new String(data),
                                header.position(),
                                header.offset(),
                                header.termOffset(),
                                header.type()));
                      },
                      FRAGMENT_LIMIT))
          .blockLast();

    } finally {
      Utils.removeFile(aeronDirName);
    }
  }
}

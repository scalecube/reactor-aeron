package reactor.aeron.pure.archive.examples.recording.simple;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.pure.archive.Utils;
import reactor.core.publisher.Flux;

public class Sender {

  private static final String OUTBOUND_CHANNEL_URI =
      new ChannelUriStringBuilder()
          .endpoint(RecordingServer.INCOMING_RECORDING_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int OUTBOUND_STREAM_ID = RecordingServer.INCOMING_RECORDING_STREAM_ID;

  private static final Duration SENT_INTERVAL = Duration.ofSeconds(1);

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
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true));
        Aeron aeron =
            Aeron.connect(
                new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()))) {

      Publication publication =
          aeron.addExclusivePublication(OUTBOUND_CHANNEL_URI, OUTBOUND_STREAM_ID);

      Flux.interval(SENT_INTERVAL)
          .map(i -> "Hello World! " + i)
          .doOnNext(message -> Utils.send(publication, message))
          .blockLast();
    } finally {
      Utils.removeFile(aeronDirName);
    }
  }
}

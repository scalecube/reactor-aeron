package reactor.aeron.pure.archive;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import reactor.aeron.Configurations;
import reactor.core.publisher.Flux;

public class SubReplayClient {

  private static final String CHANNEL =
      new ChannelUriStringBuilder()
          .controlEndpoint(Broker.BROKER_REPLAY_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int STREAM_ID =
      Broker.BROKER_REPLAY_STREAM_ID
          + 1; // todo to start from specify position (not just listen to current messages)
  private static final long RECORDING_ID = 0L; // todo NOTICE change it if needed
  private static final long POSITION = 91008; // todo NOTICE change it if needed

  public static void main(String[] args) {
    String aeronDirName = Utils.tmpFileName("aeron");

    try (MediaDriver mediaDriver =
            MediaDriver.launch(
                new Context()
                    .threadingMode(ThreadingMode.SHARED)
                    // .spiesSimulateConnection(false)
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true));
        AeronArchive aeronArchive =
            AeronArchive.connect(
                new AeronArchive.Context()
                    .controlResponseChannel("aeron:udp?endpoint=localhost:8022")
                    .controlResponseStreamId(18022)
                    .aeronDirectoryName(aeronDirName));
        Subscription subscription =
            aeronArchive.replay(
                RECORDING_ID,
                POSITION,
                Long.MAX_VALUE,
                CHANNEL,
                STREAM_ID,
                Configurations::printAvailableImage,
                Configurations::printUnavailableImage); ) {

      System.out.println("Created subscription: " + CHANNEL + ", streamId: " + STREAM_ID);

      Flux.interval(Duration.ofMillis(100))
          .doOnNext(
              i ->
                  subscription.poll(
                      (buffer, offset, length, header) -> {
                        final byte[] data = new byte[length];
                        buffer.getBytes(offset, data);

                        System.out.println(
                            String.format(
                                "Message to stream %d from session %d (%d@%d) <<%s>>, header{ pos: %s, offset: %s, termOffset: %s, type: %s}",
                                STREAM_ID,
                                header.sessionId(),
                                length,
                                offset,
                                new String(data),
                                header.position(),
                                header.offset(),
                                header.termOffset(),
                                header.type()));
                      },
                      10))
          .blockLast();

    } finally {
      Utils.removeFile(aeronDirName);
    }
  }
}

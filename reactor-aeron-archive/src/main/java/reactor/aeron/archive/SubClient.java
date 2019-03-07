package reactor.aeron.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import java.time.Duration;
import java.util.UUID;
import org.agrona.IoUtil;
import org.agrona.collections.MutableLong;
import reactor.core.publisher.Flux;

public class SubClient {

  private static MediaDriver mediaDriver;
  private static Aeron aeron;
  private static AeronArchive aeronArchive;

  public static void main(String[] args) {

    try {

      String aeronDirName = tmpFileName("aeron");

      mediaDriver =
          MediaDriver.launch(
              new Context()
                  .threadingMode(ThreadingMode.SHARED)
                  // .spiesSimulateConnection(false)
                  .errorHandler(Throwable::printStackTrace)
                  .aeronDirectoryName(aeronDirName)
                  .dirDeleteOnStart(true));

      aeron =
          Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

      aeronArchive =
          AeronArchive.connect(
              new AeronArchive.Context()
                  .aeron(aeron)
                  .controlResponseChannel("aeron:udp?endpoint=localhost:8022")
                  .controlResponseStreamId(18022)
              // .ownsAeronClient(true)
              );

      ChannelUriStringBuilder channelUri =
          new ChannelUriStringBuilder()
              .endpoint("localhost:54327")
              .reliable(Boolean.TRUE)
              .media("udp");

      int recordingStreamId = 2222;
      int listeningStreamId = recordingStreamId + 1;

      long recordingId = findLatestRecording(aeronArchive, channelUri.build(), recordingStreamId);

      long sessionId =
          aeronArchive.startReplay(
              recordingId, 0L, Long.MAX_VALUE, channelUri.build(), listeningStreamId);

      try (Subscription subscription =
          aeron.addSubscription(channelUri.sessionId((int) sessionId).build(), listeningStreamId)) {

        Flux.interval(Duration.ofMillis(100))
            .doOnNext(
                i ->
                    subscription.poll(
                        (buffer, offset, length, header) -> {
                          System.out.println(length);
                        },
                        10))
            .blockLast();
      }

    } finally {
      if (aeronArchive != null) {
        aeronArchive.close();
      }
      if (aeron != null) {
        aeron.close();
      }
      if (mediaDriver != null) {
        mediaDriver.close();
      }

      if (aeron != null) {
        IoUtil.delete(aeron.context().aeronDirectory(), true);
      }
      if (mediaDriver != null) {
        IoUtil.delete(mediaDriver.context().aeronDirectory(), true);
      }
    }
  }

  private static String tmpFileName(String value) {
    return IoUtil.tmpDirName()
        + value
        + '-'
        + System.getProperty("user.name", "default")
        + '-'
        + UUID.randomUUID().toString();
  }

  private static long findLatestRecording(AeronArchive archive, String CHANNEL, int STREAM_ID) {
    MutableLong lastRecordingId = new MutableLong();
    RecordingDescriptorConsumer consumer =
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
          lastRecordingId.set(recordingId);
        };
    final long fromRecordingId = 0L;
    final int recordCount = 100;
    int foundCount = archive.listRecordingsForUri(0L, 100, CHANNEL, STREAM_ID, consumer);
    if (foundCount == 0) {
      throw new IllegalStateException("no recordings found");
    } else {
      return lastRecordingId.get();
    }
  }
}

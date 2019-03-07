package reactor.aeron.archive;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import java.time.Duration;
import org.agrona.collections.MutableLong;
import reactor.core.publisher.Flux;

public class LocalSubClient {

  private static AeronArchive aeronArchive;

  public static void main(String[] args) {

    try {
      String aeronDirectoryName =
          "/var/folders/tx/11bk01r93rv4nhblfzfpmdhr0000gn/T/aeron-segabriel-7693ac95-9306-4744-8c43-8151167f0179";

      aeronArchive =
          AeronArchive.connect(
              new AeronArchive.Context()
                  .aeronDirectoryName(aeronDirectoryName)
                  .controlResponseChannel("aeron:udp?endpoint=localhost:8023")
                  .controlResponseStreamId(18023)
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
          aeronArchive.replay(
              recordingId, 0, Long.MAX_VALUE, channelUri.build(), listeningStreamId)) {

        Flux.interval(Duration.ofMillis(100))
            .doOnNext(
                i ->
                    subscription.poll(
                        (buffer, offset, length, header) -> {
                          final byte[] data = new byte[length];
                          buffer.getBytes(offset, data);

                          System.out.println(
                              String.format(
                                  "Message to stream %d from session %d (%d@%d) <<%s>>",
                                  listeningStreamId,
                                  header.sessionId(),
                                  length,
                                  offset,
                                  new String(data)));
                        },
                        10))
            .blockLast();
      }

    } finally {
      if (aeronArchive != null) {
        aeronArchive.close();
      }
    }
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

package reactor.aeron.pure.archive;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import java.io.File;
import java.util.UUID;
import org.agrona.IoUtil;
import reactor.core.publisher.Flux;

public class Utils {

  private Utils() {
    // no-op
  }

  /**
   * Creates tmp file with using the given value.
   *
   * @param value target.
   */
  public static String tmpFileName(String value) {
    return IoUtil.tmpDirName()
        + value
        + '-'
        + System.getProperty("user.name", "default")
        + '-'
        + UUID.randomUUID().toString();
  }

  /**
   * Creates tmp file with using the given value.
   *
   * @param value target.
   */
  public static void removeFile(String value) {
    IoUtil.delete(new File(value), true);
  }

  /**
   * Returns the list of {@link RecordingDescriptor}.
   *
   * @param aeronArchive archive client
   * @param channel target channel
   * @param channelStreamId target channel stream id
   */
  public static Flux<RecordingDescriptor> findRecording(
      final AeronArchive aeronArchive,
      String channel,
      int channelStreamId,
      int fromRecordingId,
      int count) {

    return Flux.create(
        sink -> {
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
                  sourceIdentity) ->
                  sink.next(
                      new RecordingDescriptor(
                          controlSessionId,
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
                          sourceIdentity));

          aeronArchive.listRecordingsForUri(
              fromRecordingId, count, channel, channelStreamId, consumer);

          sink.complete();
        });
  }

  public static class RecordingDescriptor {
    public final long controlSessionId;
    public final long correlationId;
    public final long recordingId;
    public final long startTimestamp;
    public final long stopTimestamp;
    public final long startPosition;
    public final long stopPosition;
    public final int initialTermId;
    public final int segmentFileLength;
    public final int termBufferLength;
    public final int mtuLength;
    public final int sessionId;
    public final int streamId;
    public final String strippedChannel;
    public final String originalChannel;
    public final String sourceIdentity;

    public RecordingDescriptor(
        long controlSessionId,
        long correlationId,
        long recordingId,
        long startTimestamp,
        long stopTimestamp,
        long startPosition,
        long stopPosition,
        int initialTermId,
        int segmentFileLength,
        int termBufferLength,
        int mtuLength,
        int sessionId,
        int streamId,
        String strippedChannel,
        String originalChannel,
        String sourceIdentity) {
      this.controlSessionId = controlSessionId;
      this.correlationId = correlationId;
      this.recordingId = recordingId;
      this.startTimestamp = startTimestamp;
      this.stopTimestamp = stopTimestamp;
      this.startPosition = startPosition;
      this.stopPosition = stopPosition;
      this.initialTermId = initialTermId;
      this.segmentFileLength = segmentFileLength;
      this.termBufferLength = termBufferLength;
      this.mtuLength = mtuLength;
      this.sessionId = sessionId;
      this.streamId = streamId;
      this.strippedChannel = strippedChannel;
      this.originalChannel = originalChannel;
      this.sourceIdentity = sourceIdentity;
    }

    @Override
    public String toString() {
      return "RecordingDescriptor{"
          + "controlSessionId="
          + controlSessionId
          + ", correlationId="
          + correlationId
          + ", recordingId="
          + recordingId
          + ", startTimestamp="
          + startTimestamp
          + ", stopTimestamp="
          + stopTimestamp
          + ", startPosition="
          + startPosition
          + ", stopPosition="
          + stopPosition
          + ", initialTermId="
          + initialTermId
          + ", segmentFileLength="
          + segmentFileLength
          + ", termBufferLength="
          + termBufferLength
          + ", mtuLength="
          + mtuLength
          + ", sessionId="
          + sessionId
          + ", streamId="
          + streamId
          + ", strippedChannel='"
          + strippedChannel
          + '\''
          + ", originalChannel='"
          + originalChannel
          + '\''
          + ", sourceIdentity='"
          + sourceIdentity
          + '\''
          + '}';
    }
  }
}

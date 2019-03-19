package reactor.aeron.pure.archive;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import java.io.File;
import java.util.UUID;
import org.agrona.IoUtil;
import org.agrona.collections.MutableLong;

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
   * Returns the latest recordingId and throw an exception otherwise.
   *
   * @param aeronArchive archive client
   * @param channel target channel
   * @param channelStreamId target channel stream id
   * @return the latest recordingId and throw an exception otherwise.
   */
  public static long findLatestRecording(
      final AeronArchive aeronArchive, String channel, int channelStreamId) {
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
        aeronArchive.listRecordingsForUri(
            fromRecordingId, recordCount, channel, channelStreamId, consumer);

    if (foundCount == 0) {
      throw new IllegalStateException("no recordings found");
    }

    return lastRecordingId.get();
  }
}

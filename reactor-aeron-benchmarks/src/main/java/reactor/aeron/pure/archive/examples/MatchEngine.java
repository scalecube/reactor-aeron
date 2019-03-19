package reactor.aeron.pure.archive.examples;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.BufferUtil;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import reactor.aeron.Configurations;
import reactor.aeron.pure.archive.Utils;

public class MatchEngine {

  static final String INCOMING_ENDPOINT = "localhost:7171";
  static final int INCOMING_STREAM_ID = 2222;
  static final String INCOMING_RECORDING_ENDPOINT = "localhost:7172";
  static final int INCOMING_RECORDING_STREAM_ID = 2224;
  static final String OUTGOING_ENDPOINT = "localhost:8181";
  static final int OUTGOING_STREAM_ID = 2223;

  private static final String INCOMING_URI =
      new ChannelUriStringBuilder()
          .endpoint(INCOMING_ENDPOINT)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final String INCOMING_RECORDING_URI =
      new ChannelUriStringBuilder()
          .controlEndpoint(INCOMING_RECORDING_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.IPC_MEDIA)
          .build();
  private static final String OUTGOING_URI =
      new ChannelUriStringBuilder()
          .controlEndpoint(OUTGOING_ENDPOINT)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media(CommonContext.UDP_MEDIA)
          .build();
  private static final int MESSAGE_SIZE = 256;
  private static final UnsafeBuffer BUFFER =
      new UnsafeBuffer(BufferUtil.allocateDirectAligned(MESSAGE_SIZE, 64));
  private static final int FRAGMENT_LIMIT = 10;

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    final AtomicBoolean running = new AtomicBoolean(true);
    SigInt.register(() -> running.set(false));

    String aeronDirName = Utils.tmpFileName("aeron");
    String archiveDirName = aeronDirName + "-archive";

    try (ArchivingMediaDriver archivingMediaDriver =
            ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .spiesSimulateConnection(true)
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
            AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(aeronDirName))) {
      print(archivingMediaDriver);

      Aeron aeron = aeronArchive.context().aeron();

      aeronArchive.startRecording(
          INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID, SourceLocation.LOCAL);
      aeronArchive.startRecording(OUTGOING_URI, OUTGOING_STREAM_ID, SourceLocation.LOCAL);

      Subscription incomingSubscription =
          aeron.addSubscription(
              INCOMING_URI,
              INCOMING_STREAM_ID,
              Configurations::printAvailableImage,
              Configurations::printUnavailableImage);

      ExclusivePublication recordingPublication =
          aeron.addExclusivePublication(INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID);

      Subscription recordingSubscription =
          aeron.addSubscription(INCOMING_RECORDING_URI, INCOMING_RECORDING_STREAM_ID);

      ExclusivePublication outgoingPublication =
          aeron.addExclusivePublication(OUTGOING_URI, OUTGOING_STREAM_ID);

      YieldingIdleStrategy idleStrategy = new YieldingIdleStrategy();

      while (running.get()) {
        int works = 0;

        works =
            incomingSubscription.poll(
                        (buffer, offset, length, header) -> {
                          while (true) {
                            long result = recordingPublication.offer(buffer, offset, length);
                            if (result > 0) {
                              break;
                            }
                            if (result == Publication.NOT_CONNECTED) {
                              System.err.println(
                                  "Offer failed because publisher is not connected to subscriber");
                            } else if (result == Publication.CLOSED) {
                              System.err.println("Offer failed publication is closed");
                            } else if (result == Publication.MAX_POSITION_EXCEEDED) {
                              System.err.println(
                                  "Offer failed due to publication reaching max position");
                            }
                          }
                        },
                        FRAGMENT_LIMIT)
                    > 0
                ? 1
                : 0;

        works +=
            recordingSubscription.poll(
                        (buffer, offset, length, header) -> {

                          // some business logic

                          if (length + Long.BYTES > MESSAGE_SIZE) {
                            System.err.println("can't publish msg with its position");
                            return;
                          }

                          final byte[] data = new byte[length];
                          buffer.getBytes(offset, data);

                          System.out.println(
                              String.format(
                                  "msg{ offset: %s, length: %s, body: %s }, header{ pos: %s, offset: %s, type: %s }, channel { stream: %s, session: %s, initialTermId: %s, termId: %s, termOffset: %s, flags: %s }",
                                  offset,
                                  length,
                                  new String(data),
                                  header.position(),
                                  header.offset(),
                                  header.type(),
                                  INCOMING_STREAM_ID,
                                  header.sessionId(),
                                  header.initialTermId(),
                                  header.termId(),
                                  header.termOffset(),
                                  header.flags()));

                          BUFFER.putLong(0, header.position());
                          BUFFER.putBytes(Long.BYTES, buffer, offset, length);
                          outgoingPublication.offer(BUFFER, 0, length + Long.BYTES);
                        },
                        FRAGMENT_LIMIT)
                    > 0
                ? 1
                : 0;

        idleStrategy.idle(works);
      }

      Thread.currentThread().join();
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

    System.out.println("Archive listen: " + INCOMING_URI);
  }
}

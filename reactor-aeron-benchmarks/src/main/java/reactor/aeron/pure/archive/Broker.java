package reactor.aeron.pure.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import reactor.aeron.Configurations;

public class Broker {

  public static final String BROKER_CONTROL_ENDPOINT = "localhost:7171";
  public static final int BROKER_CONTROL_STREAM_ID = 2222;

  public static void main(String[] args) throws Exception {
    String aeronDirName = Utils.tmpFileName("aeron");
    String archiveDirName = aeronDirName + "-archive";

    try (ArchivingMediaDriver archivingMediaDriver =
            ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    // .spiesSimulateConnection(false)
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
            AeronArchive.connect(
                new AeronArchive.Context() //
                    .aeronDirectoryName(aeronDirName)
                // .aeron(archivingMediaDriver.archive().context().aeron())
                // .controlResponseChannel("aeron:udp?endpoint=localhost:8021")
                // .controlResponseStreamId(18021)
                // .ownsAeronClient(true)
                ); ) {
      print(archivingMediaDriver);

      Aeron aeron = archivingMediaDriver.archive().context().aeron();

      ChannelUriStringBuilder channelUri =
          new ChannelUriStringBuilder()
              .endpoint(BROKER_CONTROL_ENDPOINT)
              .media(CommonContext.UDP_MEDIA);

      Subscription subscription =
          aeron.addSubscription(
              channelUri.build(),
              BROKER_CONTROL_STREAM_ID,
              image -> {
                Configurations.printAvailableImage(image);
              },
              image -> {
                Configurations.printUnavailableImage(image);
              });

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
  }
}

package reactor.aeron.archive;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.UUID;
import org.agrona.IoUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import reactor.core.publisher.Flux;

public class MessageBroker {

  public static final String MY_CHANNEL = "aeron:udp?endpoint=localhost:54327";
  public static final int MY_STREAM_ID = 1001;
  private static ArchivingMediaDriver archivingMediaDriver;

  public static void main(String[] args) throws Exception {
    String aeronDirName = tmpFileName("aeron");
    String archiveDirName = tmpFileName("archive");

    System.out.println(archiveDirName);

    try (ArchivingMediaDriver $ =
        archivingMediaDriver =
            ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .spiesSimulateConnection(false)
                    .errorHandler(Throwable::printStackTrace)
                    .aeronDirectoryName(aeronDirName)
                    .dirDeleteOnStart(true),
                new Archive.Context()

//                    .controlChannel(MY_CHANNEL)
//                    .controlStreamId(MY_STREAM_ID)

                    .aeronDirectoryName(aeronDirName)
                    .archiveDirectoryName(archiveDirName)
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .errorHandler(Throwable::printStackTrace)
                    .fileSyncLevel(0)
                    .deleteArchiveOnStart(true))) {

      MediaDriver mediaDriver = archivingMediaDriver.mediaDriver();
      Archive archive = archivingMediaDriver.archive();




      Aeron aeron =
          Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

      Subscription subscription = aeron.addSubscription(MY_CHANNEL, MY_STREAM_ID);

      IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1000);

      while (true) {
        int poll = subscription.poll((buffer, offset, length, header) -> {
          System.out.println(length);
        }, 1);

        idleStrategy.idle(poll);
      }

//      AeronArchive aeronArchive =
//          AeronArchive.connect(
//              new AeronArchive.Context()
//                  .aeron(aeron)
//                  .controlResponseChannel(MY_CHANNEL)
//                  .controlResponseStreamId(MY_STREAM_ID)
//                  .ownsAeronClient(true));


//      System.err.println("mediaDriver.aeronDirectoryName() = " + mediaDriver.aeronDirectoryName());
//      System.err.println(
//          "archive.context().aeronDirectoryName() = " + archive.context().aeronDirectoryName());
//      System.err.println(
//          "archive.context().archiveDirectoryName() = " + archive.context().archiveDirectoryName());

//      System.err.println();
//      System.err.println(
//          "aeron.context().aeronDirectoryName() = " + aeron.context().aeronDirectoryName());
//      System.err.println(
//          "aeronArchive.context().aeronDirectoryName() = "
//              + aeronArchive.context().aeronDirectoryName());

//      Thread.currentThread().join();
    } finally {
      if (archivingMediaDriver != null) {
        archivingMediaDriver.archive().context().deleteArchiveDirectory();
        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
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
}

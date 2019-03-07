package reactor.aeron.archive;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.util.UUID;
import org.agrona.IoUtil;

public class MessageBroker {

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
                    .deleteArchiveOnStart(true))) {

      MediaDriver mediaDriver = archivingMediaDriver.mediaDriver();
      Archive archive = archivingMediaDriver.archive();

      Thread.currentThread().join();
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

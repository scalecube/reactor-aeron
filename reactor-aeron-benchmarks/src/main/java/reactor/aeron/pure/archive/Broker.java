package reactor.aeron.pure.archive;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import reactor.aeron.Configurations;

public class Broker {

  private static final boolean INFO_FLAG = Configurations.INFO_FLAG;

  public static void main(String[] args) throws Exception {
    String aeronDirName = Utils.tmpFileName("aeron");
    String archiveDirName = aeronDirName + "-archive";

    ArchivingMediaDriver archivingMediaDriver = null;

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

      if (INFO_FLAG) {
        Aeron aeron = archive.context().aeron();
        aeron.context().availableImageHandler(Configurations::printAvailableImage);
        aeron.context().unavailableImageHandler(Configurations::printUnavailableImage);
      }

      System.out.println("Archive has started");
      System.out.println("Archive has started with threadingMode: " + archivingMediaDriver.archive().context().threadingMode());
      System.out.println("Archive has started with controlChannel: " + archivingMediaDriver.archive().context().controlChannel());
      System.out.println("Archive has started with controlStreamId: " + archivingMediaDriver.archive().context().controlStreamId());
      System.out.println("Archive has started with localControlChannel: " + archivingMediaDriver.archive().context().localControlChannel());
      System.out.println("Archive has started with localControlStreamId: " + archivingMediaDriver.archive().context().localControlStreamId());
      System.out.println("Archive has started with recordingEventsChannel: " + archivingMediaDriver.archive().context().recordingEventsChannel());
      System.out.println("Archive has started with recordingEventsStreamId: " + archivingMediaDriver.archive().context().recordingEventsStreamId());
      System.out.println("Archive has started with controlTermBufferSparse: " + archivingMediaDriver.archive().context().controlTermBufferSparse());
      System.out.println("Archive has started with archiveDirName: " + archive.context().archiveDirectoryName());
      System.out.println("Archive has started with aeronDirectoryName: " + mediaDriver.aeronDirectoryName());

      Thread.currentThread().join();
    } finally {
      if (archivingMediaDriver != null) {
        archivingMediaDriver.archive().context().deleteArchiveDirectory();
        archivingMediaDriver.mediaDriver().context().deleteAeronDirectory();
      }
    }
  }
}

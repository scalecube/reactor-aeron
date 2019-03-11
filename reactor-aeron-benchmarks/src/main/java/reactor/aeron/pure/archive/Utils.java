package reactor.aeron.pure.archive;

import java.io.File;
import java.util.UUID;
import org.agrona.IoUtil;

public class Utils {

  private Utils() {
    // no-op
  }

  public static String tmpFileName(String value) {
    return IoUtil.tmpDirName()
        + value
        + '-'
        + System.getProperty("user.name", "default")
        + '-'
        + UUID.randomUUID().toString();
  }

  public static void removeFile(String value) {
//    IoUtil.deleteIfExists(new File(value));
    IoUtil.delete(new File(value), true);

  }
}

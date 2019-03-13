package reactor.aeron.pure.archive;

import java.io.File;
import java.util.UUID;
import org.agrona.IoUtil;

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
}

package reactor.aeron;

import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ByteBufUtil {

  private static final Logger logger = LoggerFactory.getLogger(ByteBufUtil.class);

  private ByteBufUtil() {
    // Do not instantiate
  }

  /**
   * Try to release input object iff it's instance is of {@link ReferenceCounted} type and its
   * refCount greater than zero.
   *
   * @return true if msg release taken place
   */
  static boolean safestRelease(Object msg) {
    try {
      return (msg instanceof ReferenceCounted)
          && ((ReferenceCounted) msg).refCnt() > 0
          && ((ReferenceCounted) msg).release();
    } catch (Throwable t) {
      logger.warn("Failed to release reference counted object: {}", msg, t);
      return false;
    }
  }
}

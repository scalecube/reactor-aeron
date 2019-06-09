package reactor.aeron;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;

public interface FragmentMapper<T> {

  /**
   * Converts the given arguments to a T instance. Also can return the null value if the T instance
   * is not ready to publish. Any thrown exception inside this method will cancel polling. If the
   * mapper wished to terminate and close then a {@link AgentTerminationException} can be thrown.
   *
   * @return null to indicate no instance was currently available, a T instance otherwise.
   */
  T apply(DirectBuffer buffer, int offset, int length, Header header);
}

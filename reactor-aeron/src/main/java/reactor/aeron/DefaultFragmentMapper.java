package reactor.aeron;

import io.aeron.logbuffer.Header;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class DefaultFragmentMapper implements FragmentMapper<DirectBuffer> {

  @Override
  public DirectBuffer apply(DirectBuffer buffer, int offset, int length, Header header) {
    return new UnsafeBuffer(buffer, offset, length);
  }

  /**
   * Return function which converts incoming {@link DirectBuffer} to {@link String}.
   *
   * @return function
   */
  public static Function<DirectBuffer, String> asString() {
    return buffer -> {
      byte[] bytes = new byte[buffer.capacity()];
      buffer.getBytes(0, bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    };
  }
}

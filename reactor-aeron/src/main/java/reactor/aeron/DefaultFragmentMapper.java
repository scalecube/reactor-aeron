package reactor.aeron;

import io.aeron.logbuffer.Header;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Flux;

public class DefaultFragmentMapper implements FragmentMapper<DirectBuffer> {

  @Override
  public DirectBuffer apply(DirectBuffer buffer, int offset, int length, Header header) {
    return new UnsafeBuffer(buffer, offset, length);
  }

  public static Flux<String> asString(Flux<?> flux) {
    return flux.cast(DirectBuffer.class)
        .map(
            buffer -> {
              byte[] bytes = new byte[buffer.capacity()];
              buffer.getBytes(0, bytes);
              return new String(bytes, StandardCharsets.UTF_8);
            });
  }
}

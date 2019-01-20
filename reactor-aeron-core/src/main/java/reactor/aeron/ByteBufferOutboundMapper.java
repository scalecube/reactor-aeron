package reactor.aeron;

import java.nio.ByteBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ByteBufferOutboundMapper implements OutboundMapper<ByteBuffer> {

  @Override
  public void write(ByteBuffer content, OutboundPublication publication) {
    int length = content.remaining();
    int position = content.position();
    int limit = content.limit();

    boolean applied =
        publication.tryClaim(
            length,
            (destination, offset) -> destination.putBytes(offset, content, position, limit));
    if (!applied) {
      publication.offer(new UnsafeBuffer(content, position, limit));
    }
  }

  @Override
  public void release(ByteBuffer content) {
    // no-op
  }
}

package reactor.aeron;

import org.agrona.DirectBuffer;

public interface DirectBufferHandler<B> {

  int estimateLength(B buffer);

  DirectBuffer map(B buffer, int length);

  void dispose(B buffer);
}

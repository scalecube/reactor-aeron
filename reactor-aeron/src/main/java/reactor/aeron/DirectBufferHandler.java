package reactor.aeron;

import org.agrona.DirectBuffer;

@FunctionalInterface
public interface DirectBufferHandler<B> {

  DirectBuffer map(B buffer);

  default void dispose(B buffer) {
    // no-op
  }
}

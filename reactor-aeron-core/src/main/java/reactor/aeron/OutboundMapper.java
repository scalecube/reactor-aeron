package reactor.aeron;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public interface OutboundMapper<O> {

  void write(O content, OutboundPublication publication);

  void release(O content);

  interface OutboundPublication {

    boolean tryClaim(int length, ClaimConsumer consumer);

    default void offer(DirectBuffer buffer) {
      offer(buffer, 0, buffer.capacity());
    }

    void offer(DirectBuffer buffer, int offset, int length);

    @FunctionalInterface
    interface ClaimConsumer {

      void accept(MutableDirectBuffer target, int offset);
    }
  }
}

package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.agrona.DirectBuffer;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

// TODO think of better design for inbound -- dont allow clients cast to FragmentHandler or
// Disposable
public final class DefaultAeronInbound implements AeronInbound, FragmentHandler, Disposable {

  private final MessageSubscription messageSubscription;
  private volatile CoreSubscriber<? super ByteBuffer> destination;

  public DefaultAeronInbound(MessageSubscription messageSubscription) {
    this.messageSubscription = messageSubscription;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    ByteBuffer dstBuffer = ByteBuffer.allocate(length);
    buffer.getBytes(offset, dstBuffer, length);
    dstBuffer.flip();
    // todo see io.aeron.ControlledFragmentAssembler and its io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT
//    if (destination == null) {
//      //abort
//      return;
//    }
    destination.onNext(dstBuffer);
  }

  @Override
  public ByteBufferFlux receive() {
    // todo do we need onBackpressureBuffer?
    return new ByteBufferFlux(new FluxReceiver().onBackpressureBuffer());
  }

  @Override
  public void dispose() {
    destination.onComplete();
    destination = null;
  }

  @Override
  public boolean isDisposed() {
    return destination == null;
  }

  private class FluxReceiver extends Flux<ByteBuffer> {

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuffer> destination) {
      Objects.requireNonNull(destination);
      //todo change synchronized
      synchronized (this) {
        if (DefaultAeronInbound.this.destination != null) {
          throw new IllegalStateException("only allows one subscription");
        }
        DefaultAeronInbound.this.destination = destination;
      }

      destination.onSubscribe(messageSubscription);
    }
  }
}

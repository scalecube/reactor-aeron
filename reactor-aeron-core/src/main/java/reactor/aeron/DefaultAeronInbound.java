package reactor.aeron;

import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.DirectBuffer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

final class DefaultAeronInbound implements AeronInbound {

  private static final int MAX_FRAGMENT_LIMIT = 8192;

  private static final AtomicLongFieldUpdater<DefaultAeronInbound> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(DefaultAeronInbound.class, "requested");

  private static final AtomicReferenceFieldUpdater<DefaultAeronInbound, CoreSubscriber>
      DESTINATION_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              DefaultAeronInbound.class, CoreSubscriber.class, "destinationSubscriber");

  private final Image image;
  private final AeronEventLoop eventLoop;
  private final FluxReceive inbound = new FluxReceive();
  private final FragmentHandler fragmentHandler =
      new ImageFragmentAssembler(new InnerFragmentHandler());
  private final MessageSubscription subscription;
  private final ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;

  private volatile long requested;
  private long produced;
  private volatile CoreSubscriber<? super ByteBuf> destinationSubscriber;

  /**
   * Constructor.
   *
   * @param image image
   * @param eventLoop event loop
   * @param subscription subscription
   */
  DefaultAeronInbound(Image image, AeronEventLoop eventLoop, MessageSubscription subscription) {
    this.image = image;
    this.eventLoop = eventLoop;
    this.subscription = subscription;
  }

  @Override
  public ByteBufFlux receive() {
    return new ByteBufFlux(inbound);
  }

  int poll() {
    int r = (int) Math.min(requested, MAX_FRAGMENT_LIMIT);
    int fragments = 0;
    if (r > 0) {
      fragments = image.poll(fragmentHandler, r);
      if (produced > 0) {
        Operators.produced(REQUESTED, this, produced);
        produced = 0;
      }
    }
    return fragments;
  }

  void close() {
    if (!eventLoop.inEventLoop()) {
      throw AeronExceptions.failWithResourceDisposal("aeron inbound");
    }
    inbound.cancel();
  }

  void dispose() {
    eventLoop
        .disposeInbound(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
    if (subscription != null) {
      subscription.dispose();
    }
  }

  private class InnerFragmentHandler implements FragmentHandler {

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      produced++;

      ByteBuf byteBuf = byteBufAllocator.buffer(length);

      for (int i = offset; i < offset + length; i++) {
        byteBuf.writeByte(buffer.getByte(i));
      }

      CoreSubscriber<? super ByteBuf> destination = DefaultAeronInbound.this.destinationSubscriber;

      // TODO check on cancel?
      destination.onNext(byteBuf);
    }
  }

  private class FluxReceive extends Flux<ByteBuf> implements Subscription {

    @Override
    public void request(long n) {
      Operators.addCap(REQUESTED, DefaultAeronInbound.this, n);
    }

    @Override
    public void cancel() {
      REQUESTED.set(DefaultAeronInbound.this, 0);
      // TODO think again whether re-subscribtion is allowed; or we have to should emit cancel
      // signal upper to some conneciton shutdown hook
      CoreSubscriber destination = DESTINATION_SUBSCRIBER.getAndSet(DefaultAeronInbound.this, null);
      if (destination != null) {
        destination.onComplete();
      }
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuf> destinationSubscriber) {
      boolean destinationSet =
          DESTINATION_SUBSCRIBER.compareAndSet(
              DefaultAeronInbound.this, null, destinationSubscriber);
      if (destinationSet) {
        destinationSubscriber.onSubscribe(this);
      } else {
        Operators.error(destinationSubscriber, Exceptions.duplicateOnSubscribeException());
      }
    }
  }
}

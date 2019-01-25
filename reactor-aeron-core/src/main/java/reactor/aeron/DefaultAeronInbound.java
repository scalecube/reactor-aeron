package reactor.aeron;

import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

final class DefaultAeronInbound implements AeronInbound {

  private static final AtomicLongFieldUpdater<DefaultAeronInbound> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(DefaultAeronInbound.class, "requested");

  private static final AtomicReferenceFieldUpdater<DefaultAeronInbound, CoreSubscriber>
      DESTINATION_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              DefaultAeronInbound.class, CoreSubscriber.class, "destinationSubscriber");

  private final int fragmentLimit;
  private final Image image;
  private final AeronEventLoop eventLoop;
  private final FluxReceive inbound = new FluxReceive();
  private final FragmentHandler fragmentHandler =
      new ImageFragmentAssembler(new FragmentHandlerImpl());
  private final MessageSubscription subscription;

  private volatile long requested;
  private volatile boolean fastpath;
  private long produced;
  private volatile CoreSubscriber<? super DirectBuffer> destinationSubscriber;

  /**
   * Constructor.
   *
   * @param image image
   * @param eventLoop event loop
   * @param subscription subscription
   * @param fragmentLimit fragment limit
   */
  DefaultAeronInbound(
      Image image, AeronEventLoop eventLoop, MessageSubscription subscription, int fragmentLimit) {
    this.image = image;
    this.eventLoop = eventLoop;
    this.subscription = subscription;
    this.fragmentLimit = fragmentLimit;
  }

  @Override
  public DirectBufferFlux receive() {
    return new DirectBufferFlux(inbound);
  }

  int poll() {
    if (fastpath) {
      return image.poll(fragmentHandler, fragmentLimit);
    }
    int r = (int) Math.min(requested, fragmentLimit);
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

  private class FragmentHandlerImpl implements FragmentHandler {

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      produced++;

      CoreSubscriber<? super DirectBuffer> destination =
          DefaultAeronInbound.this.destinationSubscriber;

      destination.onNext(new UnsafeBuffer(buffer, offset, length));
    }
  }

  private class FluxReceive extends Flux<DirectBuffer> implements Subscription {

    @Override
    public void request(long n) {
      if (fastpath) {
        return;
      }
      if (n == Long.MAX_VALUE) {
        fastpath = true;
        requested = Long.MAX_VALUE;
        return;
      }
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
    public void subscribe(CoreSubscriber<? super DirectBuffer> destinationSubscriber) {
      boolean result =
          DESTINATION_SUBSCRIBER.compareAndSet(
              DefaultAeronInbound.this, null, destinationSubscriber);
      if (result) {
        destinationSubscriber.onSubscribe(this);
      } else {
        // only subscriber is allowed on receive()
        Operators.error(destinationSubscriber, Exceptions.duplicateOnSubscribeException());
      }
    }
  }
}

package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.DirectBuffer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

public class FluxReceiver extends Flux<ByteBuffer> {

  private static final Logger logger = LoggerFactory.getLogger(FluxReceiver.class);


  private final FragmentHandler source;

  private volatile long requested;
  private volatile long processed;
  private Subscriber<? super ByteBuffer> destination;
  private InnerSubscription subscription;

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuffer> destination) {
    Objects.requireNonNull(destination);
    synchronized (this) {
      if (this.destination != null && subscription.canEmit()) {
        throw new IllegalStateException("only allows one subscription");
      }
      this.destination = destination;
    }

    this.subscription = new InnerSubscription(destination);
    destination.onSubscribe(subscription);
  }

  private static class InnerSubscription implements Subscription {

    private volatile boolean erred = false;
    private volatile boolean cancelled = false;

    public InnerSubscription(Subscriber<? super ByteBuffer> destination) {

    }

    @Override
    public void request(long n) {

    }

    @Override
    public void cancel() {

    }

    private boolean canEmit() {
      return !cancelled && !erred;
    }
  }
}

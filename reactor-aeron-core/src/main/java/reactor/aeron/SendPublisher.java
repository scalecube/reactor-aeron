package reactor.aeron;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;

public class SendPublisher extends Flux<ByteBuffer> {

  private static final AtomicIntegerFieldUpdater<SendPublisher> WIP =
      AtomicIntegerFieldUpdater.newUpdater(SendPublisher.class, "wip");

  private static final int MAX_SIZE = Queues.SMALL_BUFFER_SIZE;
  private static final int REFILL_SIZE = MAX_SIZE / 2;
  // private static final int MAX_SIZE = 2;    todo just for test
  // private static final int REFILL_SIZE = 1; todo just for test
  private static final AtomicReferenceFieldUpdater<SendPublisher, Object> INNER_SUBSCRIBER =
      AtomicReferenceFieldUpdater.newUpdater(SendPublisher.class, Object.class, "innerSubscriber");
  private static final AtomicIntegerFieldUpdater<SendPublisher> TERMINATED =
      AtomicIntegerFieldUpdater.newUpdater(SendPublisher.class, "terminated");
  private final Publisher<? extends ByteBuffer> source;
  private final MessagePublication publication;
  private final AeronEventLoop eventLoop;
  private final Queue<ByteBuffer> queue;
  private final AtomicBoolean completed = new AtomicBoolean();

  @SuppressWarnings("unused")
  private volatile int terminated;

  private int pending;

  @SuppressWarnings("unused")
  private volatile int wip;

  @SuppressWarnings("unused")
  private volatile Object innerSubscriber;

  private long requested;

  private long requestedUpstream = MAX_SIZE;

  private boolean fuse;

  private final long sessionId;

  SendPublisher(
      long sessionId,
      Publisher<? extends ByteBuffer> source,
      MessagePublication publication,
      AeronEventLoop eventLoop) {
    this(sessionId, Queues.<ByteBuffer>small().get(), source, publication, eventLoop);
  }

  @SuppressWarnings("unchecked")
  SendPublisher(
      long sessionId,
      Queue<ByteBuffer> queue,
      Publisher<? extends ByteBuffer> source,
      MessagePublication publication,
      AeronEventLoop eventLoop) {
    this.sessionId = sessionId;
    this.source = source;
    this.publication = publication;
    this.queue = queue;
    this.eventLoop = eventLoop;

    fuse = queue instanceof Fuseable.QueueSubscription;
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuffer> destination) {
    InnerSubscriber innerSubscriber = new InnerSubscriber(destination);
    if (!INNER_SUBSCRIBER.compareAndSet(this, null, innerSubscriber)) {
      Operators.error(
          destination, new IllegalStateException("SendPublisher only allows one subscription"));
    } else {
      InnerSubscription innerSubscription = new InnerSubscription(innerSubscriber);
      destination.onSubscribe(innerSubscription);
      source.subscribe(innerSubscriber);
    }
  }

  private class InnerSubscriber implements Subscriber<ByteBuffer> {
    final CoreSubscriber<? super ByteBuffer> destination;
    volatile Subscription subscription;

    private InnerSubscriber(CoreSubscriber<? super ByteBuffer> destination) {
      this.destination = destination;
    }

    @Override
    public void onSubscribe(Subscription s) {
      this.subscription = s;
      s.request(MAX_SIZE);
      tryDrain();
    }

    @Override
    public void onNext(ByteBuffer t) {
      if (terminated == 0) {
        if (!fuse && !queue.offer(t)) {
          throw new IllegalStateException("missing back pressure");
        }
        tryDrain();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (TERMINATED.compareAndSet(SendPublisher.this, 0, 1)) {
        try {
          subscription.cancel();
          destination.onError(t);
        } finally {
          if (!queue.isEmpty()) {
            // todo queue.forEach(ReferenceCountUtil::safeRelease);
          }
        }
      }
    }

    @Override
    public void onComplete() {
      if (completed.compareAndSet(false, true)) {
        tryDrain();
      }
    }

    private void tryRequestMoreUpstream() {
      if (requestedUpstream <= REFILL_SIZE && subscription != null) {
        long u = MAX_SIZE - requestedUpstream;
        requestedUpstream = Operators.addCap(requestedUpstream, u);
        subscription.request(u);
      }
    }

    private void tryDrain() {
      if (wip == 0 && terminated == 0 && WIP.getAndIncrement(SendPublisher.this) == 0) {
        try {
          drain();
        } catch (Throwable t) {
          onError(t);
        }
      }
    }

    private void drain() {
      try {
        int missed = 1;
        for (; ; ) {
          long r = Math.min(requested, requestedUpstream);
          while (r-- > 0) {
            ByteBuffer poll = queue.poll();
            if (poll != null && terminated == 0) {
              pending++;

              publication
                  .enqueue(MessageType.NEXT, poll, sessionId)
                  .doOnSuccess(
                      avoid -> {
                        try {
                          if (requested != Long.MAX_VALUE) {
                            requested--;
                          }
                          requestedUpstream--;
                          pending--;

                          InnerSubscriber is =
                              (InnerSubscriber) INNER_SUBSCRIBER.get(SendPublisher.this);
                          if (is != null) {
                            is.tryRequestMoreUpstream();
                            if (pending == 0
                                && completed.get()
                                && queue.isEmpty()
                                && terminated == 0) {
                              TERMINATED.set(SendPublisher.this, 1);
                              is.destination.onComplete();
                            }
                          }
                        } finally {
                          // todo ReferenceCountUtil.safeRelease(poll);
                        }
                      })
                  // todo .subscribe(null, this::disposeCurrentDataStream);
                  .subscribe();

              tryRequestMoreUpstream();
            } else {
              break;
            }
          }

          if (terminated == 1) {
            break;
          }

          missed = WIP.addAndGet(SendPublisher.this, -missed);
          if (missed == 0) {
            break;
          }
        }
      } catch (Throwable t) {
        onError(t);
      }
    }
  }

  private class InnerSubscription implements Subscription {
    private final InnerSubscriber innerSubscriber;

    private InnerSubscription(InnerSubscriber innerSubscriber) {
      this.innerSubscriber = innerSubscriber;
    }

    @Override
    public void request(long n) {
      if (eventLoop.inEventLoop()) {
        requested = Operators.addCap(n, requested);
        innerSubscriber.tryDrain();
      } else {
        eventLoop
            .execute(
                sink -> {
                  request(n);
                  sink.success();
                })
            .subscribe(/*todo*/);
      }
    }

    @Override
    public void cancel() {
      TERMINATED.set(SendPublisher.this, 1);
      while (!queue.isEmpty()) {
        ByteBuffer poll = queue.poll();
        if (poll != null) {
          // todo ReferenceCountUtil.safeRelease(poll);
        }
      }
    }
  }
}

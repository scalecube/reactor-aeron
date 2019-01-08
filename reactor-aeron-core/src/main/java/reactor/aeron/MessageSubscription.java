package reactor.aeron;

import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

// TODO investigate why implementing org.reactivestreams.Subscription were needed
public final class MessageSubscription implements OnDisposable, org.reactivestreams.Subscription {

  private static final Logger logger = LoggerFactory.getLogger(MessageSubscription.class);

  private static final int PREFETCH = 32;

  private final AeronEventLoop eventLoop;
  private final Subscription subscription; // aeron subscription
  private final FragmentHandler fragmentHandler;
  private final Duration connectTimeout;

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private volatile boolean cancelled = false;
  private volatile long requested;
  private volatile long processed;

  /**
   * Constructor.
   *
   * @param subscription aeron subscription
   * @param options aeron options
   * @param eventLoop event loop where this {@code MessageSubscription} is assigned
   * @param fragmentHandler aeron fragment handler
   */
  public MessageSubscription(
      Subscription subscription,
      AeronOptions options,
      AeronEventLoop eventLoop,
      FragmentHandler fragmentHandler) {
    this.subscription = subscription;
    this.eventLoop = eventLoop;
    this.fragmentHandler = fragmentHandler;
    this.connectTimeout = options.connectTimeout();
  }

  /**
   * Subscrptions poll method.
   *
   * @return the number of fragments received
   */
  int poll() {
    if (canPoll()) {
      // TODO after removing reactiveStreams.Subscription removed from here:
      //  r, numOfPolled, requested; correlates with problem around model of AeronInbound
      int poll = subscription.poll(fragmentHandler, PREFETCH);
      if (poll > 1) {
        processed++;
      }
      return poll;
    }
    return 0;
  }

  @Override
  public void request(long n) {
    if (n < 0) {
      throw new IllegalStateException("n must be greater than zero");
    }
    // todo
    synchronized (this) {
      long r = requested;
      if (r != Long.MAX_VALUE && n > 0) {
        r += n;
        requested = r < 0 ? Long.MAX_VALUE : r;
      }
    }
  }

  @Override
  public void cancel() {
    // todo I'm not sure that we need to support cancel
    cancelled = true;
  }

  private boolean canPoll() {
    return !cancelled && processed < requested;
  }

  /**
   * Closes aeron {@link Subscription}. Can only be called from within {@link AeronEventLoop} worker
   * thred.
   *
   * <p><b>NOTE:</b> this method is not for public client (despite it was declared with {@code
   * public} signifier).
   */
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("Can only close aeron subscription from within event loop");
    }
    try {
      subscription.close();
      logger.debug("Disposed {}", this);
    } catch (Exception ex) {
      logger.warn("{} failed on aeron.Subscription close(): {}", this, ex.toString());
      throw Exceptions.propagate(ex);
    } finally {
      onDispose.onComplete();
    }
  }

  @Override
  public void dispose() {
    eventLoop
        .disposeSubscription(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  /**
   * Delegates to {@link Subscription#isClosed()}.
   *
   * @return {@code true} if aeron {@code Subscription} is closed, {@code false} otherwise
   */
  @Override
  public boolean isDisposed() {
    return subscription.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  /**
   * Spins (in async fashion) until {@link Subscription#isConnected()} would have returned {@code
   * true} or {@code connectTimeout} elapsed. See also {@link
   * MessageSubscription#ensureConnected0()}.
   *
   * @return mono result
   */
  public Mono<MessageSubscription> ensureConnected() {
    return Mono.defer(
        () -> {
          Duration retryInterval = Duration.ofMillis(100);
          long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();
          retryCount = Math.max(retryCount, 1);

          return ensureConnected0()
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .timeout(connectTimeout)
              .doOnError(
                  ex -> logger.warn("aeron.Subscription is not connected after several retries"))
              .thenReturn(this);
        });
  }

  private Mono<Void> ensureConnected0() {
    return Mono.defer(
        () ->
            subscription.isConnected()
                ? Mono.empty()
                : Mono.error(
                    AeronExceptions.failWithSubscription("aeron.Subscription is not connected")));
  }

  @Override
  public String toString() {
    return "MessageSubscription{sub=" + subscription.channel() + "}";
  }
}

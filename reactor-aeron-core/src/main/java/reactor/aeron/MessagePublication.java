package reactor.aeron;

import io.aeron.Publication;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.collections.ArrayUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.SignalType;

class MessagePublication implements OnDisposable, AeronResource {

  private static final Logger logger = LoggerFactory.getLogger(MessagePublication.class);

  private static final AtomicReferenceFieldUpdater<MessagePublication, PublisherProcessor[]>
      PUBLISHER_PROCESSORS =
          AtomicReferenceFieldUpdater.newUpdater(
              MessagePublication.class, PublisherProcessor[].class, "publisherProcessors");

  private final Publication publication;
  private final AeronEventLoop eventLoop;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration adminActionTimeout;

  private volatile Throwable lastError;

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private volatile PublisherProcessor[] publisherProcessors = new PublisherProcessor[0];

  /**
   * Constructor.
   *
   * @param publication aeron publication
   * @param options aeron options
   * @param eventLoop aeron event loop where this {@code MessagePublication} is assigned
   */
  MessagePublication(Publication publication, AeronOptions options, AeronEventLoop eventLoop) {
    this.publication = publication;
    this.eventLoop = eventLoop;
    this.connectTimeout = options.connectTimeout();
    this.backpressureTimeout = options.backpressureTimeout();
    this.adminActionTimeout = options.adminActionTimeout();
  }

  /**
   * Enqueues publisher for future sending.
   *
   * @param publisher abstract publisher to process messages from
   * @param bufferHandler abstract buffer handler
   * @return mono handle
   */
  <B> Mono<Void> publish(Publisher<B> publisher, DirectBufferHandler<? super B> bufferHandler) {
    return Mono.defer(
        () -> {
          PublisherProcessor<B> processor = new PublisherProcessor<>(bufferHandler, this);
          publisher.subscribe(processor);
          return processor.onDispose();
        });
  }

  /**
   * Makes a progress at processing publisher processors collection. See for details {@link
   * PublisherProcessor}.
   *
   * @return more than or equal {@code 1} - some progress was done; {@code 0} - denotes no progress
   *     was done
   */
  int publish() {

    PublisherProcessor[] oldArray = this.publisherProcessors;
    int result = 0;

    Exception ex = null;

    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < oldArray.length; i++) {
      PublisherProcessor processor = oldArray[i];

      processor.request();

      Object buffer = processor.buffer;
      if (buffer == null) {
        continue;
      }

      long r = 0;

      try {
        r = processor.publish(buffer);
      } catch (Exception e) {
        ex = e;
      }

      if (r > 0) {
        result++;
        processor.reset();
        continue;
      }

      // Handle closed publication
      if (r == Publication.CLOSED) {
        logger.warn("aeron.Publication is CLOSED: {}", this);
        ex = AeronExceptions.failWithPublication("aeron.Publication is CLOSED");
      }

      // Handle max position exceeded
      if (r == Publication.MAX_POSITION_EXCEEDED) {
        logger.warn("aeron.Publication received MAX_POSITION_EXCEEDED: {}", this);
        ex =
            AeronExceptions.failWithPublication("aeron.Publication received MAX_POSITION_EXCEEDED");
      }

      // Handle failed connection
      if (r == Publication.NOT_CONNECTED) {
        if (processor.isTimeoutElapsed(connectTimeout)) {
          logger.warn(
              "aeron.Publication failed to resolve NOT_CONNECTED within {} ms, {}",
              connectTimeout.toMillis(),
              this);
          ex =
              AeronExceptions.failWithPublication("Failed to resolve NOT_CONNECTED within timeout");
        }
      }

      // Handle backpressure
      if (r == Publication.BACK_PRESSURED) {
        if (processor.isTimeoutElapsed(backpressureTimeout)) {
          logger.warn(
              "aeron.Publication failed to resolve BACK_PRESSURED within {} ms, {}",
              backpressureTimeout.toMillis(),
              this);
          ex =
              AeronExceptions.failWithPublication(
                  "Failed to resolve BACK_PRESSURED within timeout");
        }
      }

      // Handle admin action
      if (r == Publication.ADMIN_ACTION) {
        if (processor.isTimeoutElapsed(adminActionTimeout)) {
          logger.warn(
              "aeron.Publication failed to resolve ADMIN_ACTION within {} ms, {}",
              adminActionTimeout.toMillis(),
              this);
          ex = AeronExceptions.failWithPublication("Failed to resolve ADMIN_ACTION within timeout");
        }
      }

      if (ex != null) {
        break;
      }
    }

    if (ex != null) {
      lastError = ex;
      dispose();
    }

    return result;
  }

  @Override
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw AeronExceptions.failWithResourceDisposal("aeron publication");
    }
    try {
      publication.close();
      logger.debug("Disposed {}", this);
    } catch (Exception ex) {
      logger.warn("{} failed on aeron.Publication close(): {}", this, ex.toString());
      throw Exceptions.propagate(ex);
    } finally {
      disposeProcessors();
      onDispose.onComplete();
    }
  }

  /**
   * Delegates to {@link Publication#sessionId()}.
   *
   * @return aeron {@code Publication} sessionId.
   */
  int sessionId() {
    return publication.sessionId();
  }

  /**
   * Delegates to {@link Publication#isClosed()}.
   *
   * @return {@code true} if aeron {@code Publication} is closed, {@code false} otherwise
   */
  @Override
  public boolean isDisposed() {
    return publication.isClosed();
  }

  @Override
  public void dispose() {
    eventLoop
        .dispose(this)
        .subscribe(
            null,
            th -> {
              // no-op
            });
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  /**
   * Spins (in async fashion) until {@link Publication#isConnected()} would have returned {@code
   * true} or {@code connectTimeout} elapsed. See also {@link
   * MessagePublication#ensureConnected0()}.
   *
   * @return mono result
   */
  Mono<MessagePublication> ensureConnected() {
    return Mono.defer(
        () -> {
          Duration retryInterval = Duration.ofMillis(100);
          long retryCount = Math.max(connectTimeout.toMillis() / retryInterval.toMillis(), 1);

          return ensureConnected0()
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .doOnError(
                  ex -> logger.warn("aeron.Publication is not connected after several retries"))
              .thenReturn(this);
        });
  }

  private Mono<Void> ensureConnected0() {
    return Mono.defer(
        () ->
            publication.isConnected()
                ? Mono.empty()
                : Mono.error(
                    AeronExceptions.failWithPublication("aeron.Publication is not connected")));
  }

  private void disposeProcessors() {
    PublisherProcessor[] oldArray = this.publisherProcessors;
    this.publisherProcessors = new PublisherProcessor[0];
    for (PublisherProcessor processor : oldArray) {
      try {
        processor.cancel();
        processor.onParentError(
            Optional.ofNullable(lastError)
                .orElse(AeronExceptions.failWithCancel("PublisherProcessor has been cancelled")));
      } catch (Exception ex) {
        // no-op
      }
    }
  }

  @Override
  public String toString() {
    return "MessagePublication{pub=" + publication.channel() + "}";
  }

  private static class PublisherProcessor<B> extends BaseSubscriber<B> implements OnDisposable {

    private final DirectBufferHandler<? super B> bufferHandler;
    private final MessagePublication parent;

    private long start;
    private boolean requested;

    private final MonoProcessor<Void> onDispose = MonoProcessor.create();

    private volatile B buffer;
    private volatile Throwable error;

    PublisherProcessor(
        DirectBufferHandler<? super B> bufferHandler, MessagePublication messagePublication) {
      this.bufferHandler = bufferHandler;
      this.parent = messagePublication;
      addSelf();
    }

    @Override
    public Mono<Void> onDispose() {
      return onDispose;
    }

    void request() {
      if (requested || isDisposed()) {
        return;
      }

      Subscription upstream = upstream();
      if (upstream != null) {
        requested = true;
        upstream.request(1);
      }
    }

    void onParentError(Throwable throwable) {
      resetBuffer();
      onDispose.onError(throwable);
    }

    void reset() {
      resetBuffer();

      if (isDisposed()) {
        removeSelf();
        if (error != null) {
          onDispose.onError(error);
        } else {
          onDispose.onComplete();
        }
        return;
      }

      start = 0;
      requested = false;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      // no-op
    }

    @Override
    protected void hookOnNext(B value) {
      if (buffer != null) {
        try {
          bufferHandler.dispose(value);
        } catch (Exception ex) {
          logger.warn("Failed to release buffer: {}", value, ex);
        }
        throw Exceptions.failWithOverflow(
            "PublisherProcessor is overrun by more signals than expected");
      }
      buffer = value;
    }

    @Override
    protected void hookOnError(Throwable throwable) {
      error = throwable;
    }

    @Override
    protected void hookFinally(SignalType type) {
      if (buffer == null) {
        reset();
      }
    }

    long publish(B buffer) {
      if (start == 0) {
        start = System.currentTimeMillis();
      }
      int length = bufferHandler.estimateLength(buffer);
      return parent.publication.offer(bufferHandler.map(buffer, length));
    }

    boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void resetBuffer() {
      B oldBuffer = buffer;
      buffer = null;
      if (oldBuffer != null) {
        try {
          bufferHandler.dispose(oldBuffer);
        } catch (Exception ex) {
          logger.warn("Failed to release buffer: {}", oldBuffer, ex);
        }
      }
    }

    private void addSelf() {
      PublisherProcessor[] oldArray;
      PublisherProcessor[] newArray;
      do {
        oldArray = parent.publisherProcessors;
        newArray = ArrayUtil.add(oldArray, this);
      } while (!PUBLISHER_PROCESSORS.compareAndSet(parent, oldArray, newArray));
    }

    private void removeSelf() {
      PublisherProcessor[] oldArray;
      PublisherProcessor[] newArray;
      do {
        oldArray = parent.publisherProcessors;
        newArray = ArrayUtil.remove(oldArray, this);
      } while (!PUBLISHER_PROCESSORS.compareAndSet(parent, oldArray, newArray));
    }
  }
}

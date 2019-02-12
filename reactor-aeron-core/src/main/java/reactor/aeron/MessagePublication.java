package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.time.Duration;
import java.util.Queue;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.jmx.MessagePublicationMBean;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

class MessagePublication implements MessagePublicationMBean, OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(MessagePublication.class);

  private static final int QUEUE_CAPACITY = 8192;

  private final Publication publication;
  private final AeronEventLoop eventLoop;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration adminActionTimeout;
  private final int writeLimit;

  private final Queue<PublishTask> publishTasks =
      new ManyToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

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
    this.writeLimit = options.resources().writeLimit();
  }

  /**
   * Enqueues content for future sending.
   *
   * @param buffer abstract buffer to send
   * @param bufferHandler abstract buffer handler
   * @return mono handle
   */
  <B> Mono<Void> publish(B buffer, DirectBufferHandler<? super B> bufferHandler) {
    return Mono.create(
        sink -> {
          boolean result = false;
          if (!isDisposed()) {
            result =
                publishTasks.offer(
                    PublishTask.newInstance(publication, buffer, bufferHandler, sink));
          }
          if (!result) {
            sink.error(AeronExceptions.failWithPublicationUnavailable());
          }
        });
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return more than or equal {@code 1} - some progress was done; {@code 0} - denotes no progress
   *     was done
   */
  int proceed() {
    int result = 0;
    for (int i = 0, current; i < writeLimit; i++) {
      current = proceed0();
      if (current < 1) {
        break;
      }
      result += current;
    }
    return result;
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return {@code 1} - some progress was done; {@code 0} - denotes no progress was done
   */
  private int proceed0() {
    Exception ex = null;

    PublishTask task = publishTasks.peek();
    if (task == null) {
      return 0;
    }

    long result = 0;

    try {
      result = task.publish();
    } catch (Exception e) {
      ex = e;
    }

    if (result > 0) {
      publishTasks.poll();
      task.success();
      return 1;
    }

    // Handle closed publication
    if (result == Publication.CLOSED) {
      logger.warn("aeron.Publication is CLOSED: {}", this);
      dispose();
      return 0;
    }

    // Handle max position exceeded
    if (result == Publication.MAX_POSITION_EXCEEDED) {
      logger.warn("aeron.Publication received MAX_POSITION_EXCEEDED: {}", this);
      dispose();
      return 0;
    }

    // Handle failed connection
    if (result == Publication.NOT_CONNECTED) {
      if (task.isTimeoutElapsed(connectTimeout)) {
        logger.warn(
            "aeron.Publication failed to resolve NOT_CONNECTED within {} ms, {}",
            connectTimeout.toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve NOT_CONNECTED within timeout");
      }
    }

    // Handle backpressure
    if (result == Publication.BACK_PRESSURED) {
      if (task.isTimeoutElapsed(backpressureTimeout)) {
        logger.warn(
            "aeron.Publication failed to resolve BACK_PRESSURED within {} ms, {}",
            backpressureTimeout.toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve BACK_PRESSURED within timeout");
      }
    }

    // Handle admin action
    if (result == Publication.ADMIN_ACTION) {
      if (task.isTimeoutElapsed(adminActionTimeout)) {
        logger.warn(
            "aeron.Publication failed to resolve ADMIN_ACTION within {} ms, {}",
            adminActionTimeout.toMillis(),
            this);
        ex = AeronExceptions.failWithPublication("Failed to resolve ADMIN_ACTION within timeout");
      }
    }

    if (ex != null) {
      // Consume task and notify subscriber(s)
      publishTasks.poll();
      task.error(ex);
    }

    return 0;
  }

  void close() {
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
      disposePublishTasks();
      onDispose.onComplete();
    }
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
        .disposePublication(this)
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

  private void disposePublishTasks() {
    PublishTask task;
    while ((task = publishTasks.poll()) != null) {
      try {
        task.error(AeronExceptions.failWithCancel("PublishTask has cancelled"));
      } catch (Exception ex) {
        // no-op
      }
    }
  }

  @Override
  public String toString() {
    return "MessagePublication{pub=" + publication.channel() + "}";
  }

  @Override
  public String getName() {
    return Integer.toHexString(publication.sessionId())
        + "_"
        + Integer.toHexString(publication.hashCode());
  }

  @Override
  public int getSessionId() {
    return publication.sessionId();
  }

  @Override
  public String getPublicationChannel() {
    return publication.channel();
  }

  @Override
  public String getEventLoopName() {
    return eventLoop.toString();
  }

  @Override
  public int getWriteLimit() {
    return writeLimit;
  }

  @Override
  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  @Override
  public Duration getBackpressureTimeout() {
    return backpressureTimeout;
  }

  @Override
  public Duration getAdminActionTimeout() {
    return adminActionTimeout;
  }

  @Override
  public int getPublishTasksCount() {
    return publishTasks.size();
  }

  /**
   * Publish task.
   *
   * <p>Resident of {@link #publishTasks} queue.
   */
  private static class PublishTask<B> {

    private static final ThreadLocal<BufferClaim> bufferClaims =
        ThreadLocal.withInitial(BufferClaim::new);

    private Publication publication;
    private B buffer;
    private DirectBufferHandler<B> bufferHandler;
    private MonoSink<Void> sink;
    private volatile boolean isDisposed;

    private long start;

    private static <B> PublishTask<B> newInstance(
        Publication publication,
        B buffer,
        DirectBufferHandler<B> bufferHandler,
        MonoSink<Void> sink) {

      PublishTask<B> task = new PublishTask<>();

      task.publication = publication;
      task.buffer = buffer;
      task.bufferHandler = bufferHandler;
      task.isDisposed = false;
      task.start = 0;
      task.sink = sink;
      task.sink.onDispose(task::onDispose);

      return task;
    }

    private long publish() {
      if (isDisposed) {
        return 1;
      }

      if (start == 0) {
        start = System.currentTimeMillis();
      }

      int length = bufferHandler.estimateLength(buffer);

      if (length < publication.maxPayloadLength()) {
        BufferClaim bufferClaim = bufferClaims.get();
        long result = publication.tryClaim(length, bufferClaim);
        if (result > 0) {
          try {
            MutableDirectBuffer directBuffer = bufferClaim.buffer();
            int offset = bufferClaim.offset();
            bufferHandler.write(directBuffer, offset, buffer, length);
            bufferClaim.commit();
          } catch (Exception ex) {
            bufferClaim.abort();
            throw Exceptions.propagate(ex);
          }
        }
        return result;
      } else {
        return publication.offer(bufferHandler.map(buffer, length));
      }
    }

    private boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void success() {
      if (!isDisposed) {
        sink.success();
      }
      bufferHandler.dispose(buffer);
    }

    private void error(Throwable ex) {
      if (!isDisposed) {
        sink.error(ex);
      }
      bufferHandler.dispose(buffer);
    }

    private void onDispose() {
      if (!isDisposed) {
        isDisposed = true;
      }
    }
  }
}

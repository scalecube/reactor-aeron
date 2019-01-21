package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Queue;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.OutboundMapper.OutboundPublication;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

class MessagePublication implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(MessagePublication.class);

  private final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

  private final Publication publication;
  private final AeronEventLoop eventLoop;
  private final Duration connectTimeout;
  private final Duration backpressureTimeout;
  private final Duration adminActionTimeout;

  private final OutboundMapper<ByteBuffer> mapper = new ByteBufferOutboundMapper();

  private final Queue<PublishTask> publishTasks = new ManyToOneConcurrentLinkedQueue<>();

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
  }

  /**
   * Enqueues buffer for future sending.
   *
   * @param buffer buffer
   * @return mono handle
   */
  Mono<Void> publish(ByteBuffer buffer) {
    return Mono.create(
        sink -> {
          boolean result = false;
          if (!isDisposed()) {
            result = publishTasks.offer(new PublishTask(buffer, sink));
          }
          if (!result) {
            sink.error(AeronExceptions.failWithPublicationUnavailable());
          }
        });
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return 1 - some progress was done; 0 - denotes no progress was done
   */
  int proceed() {
    PublishTask task = publishTasks.peek();
    if (task == null) {
      return 0;
    }

    long result = task.publish();
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

    Exception ex = null;

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
          long retryCount = connectTimeout.toMillis() / retryInterval.toMillis();
          retryCount = Math.max(retryCount, 1);

          return ensureConnected0()
              .retryBackoff(retryCount, retryInterval, retryInterval)
              .timeout(connectTimeout)
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

  /**
   * Publish task.
   *
   * <p>Resident of {@link #publishTasks} queue.
   */
  private class PublishTask implements OutboundPublication {

    private final ByteBuffer msgBody;
    private final MonoSink<Void> sink;
    private volatile boolean isDisposed = false;

    private long start;
    private long result;

    private PublishTask(ByteBuffer msgBody, MonoSink<Void> sink) {
      this.msgBody = msgBody;
      this.sink =
          sink.onDispose(
              () -> {
                if (!isDisposed) {
                  isDisposed = true;
                  mapper.release(msgBody);
                }
              });
    }

    private long publish() {
      if (isDisposed) {
        return 1;
      }

      if (start == 0) {
        start = System.currentTimeMillis();
      }

      mapper.write(msgBody, this);
      return result;
    }

    private boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void success() {
      if (!isDisposed) {
        sink.success();
      }
    }

    private void error(Throwable ex) {
      if (!isDisposed) {
        sink.error(ex);
      }
    }

    @Override
    public boolean tryClaim(int length, ClaimConsumer consumer) {
      boolean applied = length < publication.maxPayloadLength();
      if (applied) {
        BufferClaim bufferClaim = bufferClaims.get();
        long streamPosition = publication.tryClaim(length, bufferClaim);
        if (streamPosition > 0) {
          try {
            MutableDirectBuffer dstBuffer = bufferClaim.buffer();
            int offset = bufferClaim.offset();
            consumer.accept(dstBuffer, offset);
            bufferClaim.commit();
            result = streamPosition;
          } catch (Exception ex) {
            bufferClaim.abort();
            // todo maybe we need do result = -100500, but current state:
            result = streamPosition;
          }
        }
      }
      return applied;
    }

    @Override
    public void offer(DirectBuffer buffer, int offset, int length) {
      result = publication.offer(buffer, offset, length);
    }
  }
}

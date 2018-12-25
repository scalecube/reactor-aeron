package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class MessagePublication implements OnDisposable, AutoCloseable {

  private static final Logger logger = Loggers.getLogger(MessagePublication.class);

  private final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

  private final String category;
  private final int mtuLength;
  private final Publication publication;
  private final AeronOptions options;
  private final AeronEventLoop eventLoop;

  private final ManyToOneConcurrentLinkedQueue<PublishTask> publishTasks =
      new ManyToOneConcurrentLinkedQueue<>();

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param category category
   * @param publication publication
   */
  MessagePublication(
      String category,
      int mtuLength,
      Publication publication,
      AeronOptions options,
      AeronEventLoop eventLoop) {
    this.category = category;
    this.mtuLength = mtuLength;
    this.publication = publication;
    this.options = options;
    this.eventLoop = eventLoop;
  }

  /**
   * Enqueues buffer for future sending.
   *
   * @param messageType message type
   * @param buffer buffer
   * @param sessionId session id
   * @return mono handle
   */
  public Mono<Void> enqueue(MessageType messageType, ByteBuffer buffer, long sessionId) {
    return Mono.create(
        sink -> {
          boolean result = false;
          if (!isDisposed()) {
            result = publishTasks.offer(new PublishTask(messageType, buffer, sessionId, sink));
          }
          if (!result) {
            sink.error(Exceptions.failWithRejected());
          }
        });
  }

  /**
   * Proceed with processing of tasks.
   *
   * @return 1 - some progress was done; 0 - denotes no progress was done
   */
  public int proceed() {
    PublishTask task = publishTasks.peek();
    if (task == null) {
      return 0;
    }

    long result = task.run();
    if (result > 0) {
      publishTasks.poll();
      task.success();
      return 1;
    }

    // Handle closed publoication
    if (result == Publication.CLOSED) {
      logger.warn("[{}] Publication CLOSED: {}", category, toString());
      dispose();
      return 0;
    }

    Exception ex = null;

    // Handle failed connection
    if (result == Publication.NOT_CONNECTED) {
      if (task.isTimeoutElapsed(options.connectTimeout())) {
        logger.warn(
            "[{}] Publication NOT_CONNECTED: {} during {} millis",
            category,
            toString(),
            options.connectTimeout());
        ex = new RuntimeException("Failed to connect within timeout");
      }
    }

    // Handle backpressure
    if (result == Publication.BACK_PRESSURED) {
      if (task.isTimeoutElapsed(options.backpressureTimeout())) {
        logger.warn(
            "[{}] Publication BACK_PRESSURED during {}: {}",
            category,
            toString(),
            options.backpressureTimeout());
        ex = new RuntimeException("Failed to resolve backpressure within timeout");
      }
    }

    // Handle admin action
    if (result == Publication.ADMIN_ACTION) {
      if (task.isTimeoutElapsed(options.connectTimeout())) {
        logger.warn(
            "[{}] Publication ADMIN_ACTION: {} during {} millis",
            category,
            toString(),
            options.connectTimeout());
        ex = new RuntimeException("Failed to resolve admin_action within timeout");
      }
    }

    if (ex != null) {
      publishTasks.poll();
      task.error(ex);
    }

    return 0;
  }

  @Override
  public String toString() {
    return AeronUtils.format(publication);
  }

  @Override
  public void close() {
    if (!eventLoop.inEventLoop()) {
      throw new IllegalStateException("Can only close aeron publication from within event loop");
    }
    try {
      publication.close();
    } finally {
      logger.debug("[{}] aeron.Publication closed: {}", category, toString());
      disposePublishTasks();
      onDispose.onComplete();
    }
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
  public boolean isDisposed() {
    return publication.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  public boolean isConnected() {
    return publication.isConnected();
  }

  private void disposePublishTasks() {
    for (; ; ) {
      PublishTask task = publishTasks.poll();
      if (task == null) {
        break;
      }
      task.error(new RuntimeException("Publication closed"));
    }
  }

  private class PublishTask {

    private final MessageType msgType;
    private final ByteBuffer msgBody;
    private final long sessionId;
    private final MonoSink<Void> sink;

    private long start;

    private PublishTask(
        MessageType msgType, ByteBuffer msgBody, long sessionId, MonoSink<Void> sink) {
      this.msgType = msgType;
      this.msgBody = msgBody;
      this.sessionId = sessionId;
      this.sink = sink;
    }

    private long run() {
      if (start == 0) {
        start = System.currentTimeMillis();
      }
      int capacity = msgBody.remaining() + Protocol.HEADER_SIZE;
      if (capacity < mtuLength) {
        BufferClaim bufferClaim = bufferClaims.get();
        long result = publication.tryClaim(capacity, bufferClaim);
        if (result > 0) {
          try {
            MutableDirectBuffer buffer = bufferClaim.buffer();
            int index = bufferClaim.offset();
            index = Protocol.putHeader(buffer, index, msgType, sessionId);
            buffer.putBytes(index, msgBody, msgBody.position(), msgBody.limit());
            bufferClaim.commit();
          } catch (Exception ex) {
            bufferClaim.abort();
            throw new RuntimeException("Unexpected exception", ex);
          }
        }
        return result;
      } else {
        UnsafeBuffer buffer =
            new UnsafeBuffer(new byte[msgBody.remaining() + Protocol.HEADER_SIZE]);
        int index = Protocol.putHeader(buffer, 0, msgType, sessionId);
        buffer.putBytes(index, msgBody, msgBody.remaining());
        msgBody.flip();
        return publication.offer(buffer);
      }
    }

    private boolean isTimeoutElapsed(Duration timeout) {
      return System.currentTimeMillis() - start > timeout.toMillis();
    }

    private void success() {
      sink.success();
    }

    private void error(Throwable ex) {
      sink.error(ex);
    }
  }
}

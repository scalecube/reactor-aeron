package reactor.aeron;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.client.AeronClientOptions;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class DefaultMessagePublication implements OnDisposable, MessagePublication {

  private static final Logger logger = Loggers.getLogger(DefaultMessagePublication.class);

  private final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

  private final Publication publication;
  private final String category;

  private final AeronEventLoop eventLoop;
  private final AeronClientOptions options;

  private final ManyToOneConcurrentLinkedQueue<PublishTask> publishTasks =
      new ManyToOneConcurrentLinkedQueue<>();

  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Constructor.
   *
   * @param publication publication
   * @param category category
   */
  public DefaultMessagePublication(
      AeronEventLoop eventLoop,
      Publication publication,
      String category,
      AeronClientOptions options) {
    this.eventLoop = eventLoop;
    this.publication = publication;
    this.category = category;
    this.options = options;
  }

  @Override
  public Mono<Void> enqueue(MessageType msgType, ByteBuffer msgBody, long sessionId) {
    return Mono.create(
        sink -> {
          boolean result = false;
          if (!isDisposed()) {
            result = publishTasks.offer(new PublishTask(msgType, msgBody, sessionId, sink));
          }
          if (!result) {
            sink.error(Exceptions.failWithRejected());
          }
        });
  }

  @Override
  public boolean proceed() {
    PublishTask task = publishTasks.peek();
    if (task == null) {
      return false;
    }

    long result = task.publish();
    if (result > 0) {
      publishTasks.poll();
      return true;
    }

    // Handle closed publoication
    if (result == Publication.CLOSED) {
      logger.warn("[{}] Publication CLOSED: {}", category, toString());
      dispose();
      return false;
    }

    Exception ex = null;

    // Handle failed connection
    if (result == Publication.NOT_CONNECTED) {
      if (task.isTimeoutElapsed(options.connectTimeoutMillis())) {
        logger.warn(
            "[{}] Publication NOT_CONNECTED: {} during {} millis",
            category,
            toString(),
            options.connectTimeoutMillis());
        ex = new RuntimeException("Failed to connect withinh timouet");
      }
    }

    // Handle backpressure
    if (result == Publication.BACK_PRESSURED) {
      if (task.isTimeoutElapsed(options.backpressureTimeoutMillis())) {
        logger.warn(
            "[{}] Publication BACK_PRESSURED during {} millis: {}",
            category,
            toString(),
            options.backpressureTimeoutMillis());
        ex = new RuntimeException("Failed to resolve backpressure withinh timouet");
      }
    }

    // Handle admin action
    if (result == Publication.ADMIN_ACTION) {
      if (task.isTimeoutElapsed(options.connectTimeoutMillis())) {
        logger.warn(
            "[{}] Publication ADMIN_ACTION: {} during {} millis",
            category,
            toString(),
            options.connectTimeoutMillis());
        ex = new RuntimeException("Failed to resolve admin_action withinh timouet");
      }
    }

    if (ex != null) {
      publishTasks.poll();
      task.error(ex);
    }

    return false;
  }

  @Override
  public String toString() {
    return AeronUtils.format(publication);
  }

  @Override
  public void close() {
    if (!isDisposed()) {
      try {
        publication.close();
      } finally {
        disposePublishTasks();
        onDispose.onComplete();
      }
    }
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      eventLoop.dispose(this).subscribe();
    }
  }

  @Override
  public boolean isDisposed() {
    return publication.isClosed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
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
    private final long start = System.currentTimeMillis();

    private PublishTask(
        MessageType msgType, ByteBuffer msgBody, long sessionId, MonoSink<Void> sink) {
      this.msgType = msgType;
      this.msgBody = msgBody;
      this.sessionId = sessionId;
      this.sink = sink;
    }

    private boolean isTimeoutElapsed(int timeout) {
      return System.currentTimeMillis() - start > timeout;
    }

    private void error(Throwable th) {
      sink.error(th);
    }

    private long publish() {
      int mtuLength = options.mtuLength();
      int capacity = msgBody.remaining() + Protocol.HEADER_SIZE;
      if (capacity < mtuLength) {
        BufferClaim bufferClaim = bufferClaims.get();
        long result = execute(() -> publication.tryClaim(capacity, bufferClaim));
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
        return execute(() -> publication.offer(buffer));
      }
    }
  }
}

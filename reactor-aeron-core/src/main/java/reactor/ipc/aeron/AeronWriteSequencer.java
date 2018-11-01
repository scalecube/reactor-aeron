package reactor.ipc.aeron;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import reactor.core.scheduler.Scheduler;
import reactor.util.Logger;
import reactor.util.Loggers;

final class AeronWriteSequencer extends WriteSequencer<ByteBuffer> {

  private static final Logger logger = Loggers.getLogger(AeronWriteSequencer.class);

  private final long sessionId;

  private final InnerSubscriber<ByteBuffer> inner;

  private final Consumer<Throwable> errorHandler;

  AeronWriteSequencer(
      Scheduler scheduler, String category, MessagePublication publication, long sessionId) {
    super(
        scheduler,
        discardedPublisher -> {
          // no-op
        });
    this.sessionId = sessionId;
    this.errorHandler = th -> logger.error("[{}] Unexpected exception", category, th);
    this.inner = new SignalSender(this, publication, this.sessionId);
  }

  @Override
  Consumer<Throwable> getErrorHandler() {
    return errorHandler;
  }

  @Override
  InnerSubscriber<ByteBuffer> getInner() {
    return inner;
  }

  static class SignalSender extends InnerSubscriber<ByteBuffer> {

    private final long sessionId;

    private final MessagePublication publication;

    SignalSender(AeronWriteSequencer sequencer, MessagePublication publication, long sessionId) {
      super(sequencer, 16);

      this.sessionId = sessionId;
      this.publication = publication;
    }

    @Override
    void doOnSubscribe() {
      request(Long.MAX_VALUE);
    }

    @Override
    void doOnNext(ByteBuffer byteBuffer) {
      Exception cause = null;
      long result = 0;
      try {
        result = publication.publish(MessageType.NEXT, byteBuffer, sessionId);
        if (result > 0) {
          return;
        }
      } catch (Exception ex) {
        cause = ex;
      }

      cancel();

      try{

        promise.error(
            new Exception(
                "Failed to publish signal into session with Id: " + sessionId + ", result: " + result,
                cause));
      } catch (Exception e) {
        e.printStackTrace();
      }

      scheduleNextPublisherDrain();
    }

    @Override
    void doOnError(Throwable t) {
      promise.error(t);

      scheduleNextPublisherDrain();
    }

    @Override
    void doOnComplete() {
      promise.success();

      scheduleNextPublisherDrain();
    }
  }
}

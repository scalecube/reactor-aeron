package reactor.aeron;

import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

final class AeronWriteSequencer {

  private final MessagePublication publication;

  AeronWriteSequencer(MessagePublication publication) {
    this.publication = Objects.requireNonNull(publication, "message publication must be present");
  }

  /**
   * Adds a client defined data publisher to {@link AeronWriteSequencer} instance.
   *
   * @param <B> abstract buffer type (comes from client code)
   * @param publisher {@link DirectBuffer} data publisher
   * @param bufferHandler abstract buffer type handler {@link DirectBufferHandler}
   * @return mono result
   */
  <B> Mono<Void> write(Publisher<B> publisher, DirectBufferHandler<? super B> bufferHandler) {
    Objects.requireNonNull(publisher, "publisher must be not null");

    return Mono.defer(
        () -> {
          if (publication.isDisposed()) {
            return Mono.error(AeronExceptions.failWithPublicationUnavailable());
          }
          if (publisher instanceof Flux) {
            return Flux.from(publisher)
                .flatMap(
                    buffer -> publication.publish(buffer, bufferHandler),
                    publication.requestPendingCount())
                .takeUntilOther(onPublicationDispose())
                .then();
          }
          return Mono.from(publisher)
              .flatMap(buffer -> publication.publish(buffer, bufferHandler))
              .takeUntilOther(onPublicationDispose())
              .then();
        });
  }

  /**
   * Adds a client defined data publisher to {@link AeronWriteSequencer} instance.
   *
   * @param publisher {@link DirectBuffer} data publisher
   * @return mono result
   */
  Mono<Void> write(Publisher<DirectBuffer> publisher) {
    return write(publisher, DirectBufferHandlerImpl.DEFAULT_INSTANCE);
  }

  private Mono<Void> onPublicationDispose() {
    return publication
        .onDispose()
        .then(Mono.defer(() -> Mono.error(AeronExceptions.failWithPublicationUnavailable())));
  }

  private static class DirectBufferHandlerImpl implements DirectBufferHandler<DirectBuffer> {

    private static final DirectBufferHandlerImpl DEFAULT_INSTANCE = new DirectBufferHandlerImpl();

    @Override
    public int estimateLength(DirectBuffer buffer) {
      return buffer.capacity();
    }

    @Override
    public void write(
        MutableDirectBuffer dstBuffer, int index, DirectBuffer srcBuffer, int length) {
      dstBuffer.putBytes(index, srcBuffer, 0, length);
    }

    @Override
    public DirectBuffer map(DirectBuffer buffer, int length) {
      return new UnsafeBuffer(buffer, 0, length);
    }

    @Override
    public void dispose(DirectBuffer buffer) {
      // no-op
    }
  }
}

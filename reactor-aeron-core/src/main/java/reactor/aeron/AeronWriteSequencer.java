package reactor.aeron;

import io.netty.buffer.ByteBuf;
import java.util.Objects;
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
   * @param publisher data publisher
   * @return mono handle
   */
  Mono<Void> write(Publisher<? extends ByteBuf> publisher) {
    Objects.requireNonNull(publisher, "publisher must be not null");

    return Mono.defer(
        () -> {
          if (publication.isDisposed()) {
            return Mono.error(AeronExceptions.failWithPublicationUnavailable());
          }
          if (publisher instanceof Flux) {
            return Flux.from(publisher)
                .flatMap(publication::publish)
                .takeUntilOther(onPublicationDispose())
                .then();
          }
          return Mono.from(publisher)
              .flatMap(publication::publish)
              .takeUntilOther(onPublicationDispose())
              .then();
        });
  }

  private Mono<Void> onPublicationDispose() {
    return publication
        .onDispose()
        .then(Mono.defer(() -> Mono.error(AeronExceptions.failWithPublicationUnavailable())));
  }
}

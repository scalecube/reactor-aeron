package reactor.aeron;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

final class AeronOutboundThen implements AeronOutbound {

  private final Mono<Void> thenMono;
  private final AeronOutbound source;

  AeronOutboundThen(AeronOutbound source, Publisher<Void> thenPublisher) {
    Mono<Void> parentMono = source.then();
    this.source = source;
    if (parentMono == Mono.<Void>empty()) {
      this.thenMono = Mono.from(thenPublisher);
    } else {
      this.thenMono = parentMono.thenEmpty(thenPublisher);
    }
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuf> dataStream) {
    return source.send(dataStream);
  }

  @Override
  public AeronOutbound sendString(Publisher<String> dataStream) {
    return source.sendString(dataStream);
  }

  @Override
  public Mono<Void> then() {
    return thenMono;
  }
}

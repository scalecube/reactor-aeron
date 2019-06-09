package reactor.aeron;

import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
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
  public <B> AeronOutbound send(
      Publisher<B> dataStream, DirectBufferHandler<? super B> bufferHandler) {
    return source.send(dataStream, bufferHandler);
  }

  @Override
  public AeronOutbound send(Publisher<DirectBuffer> dataStream) {
    return source.send(dataStream);
  }

  @Override
  public AeronOutbound sendBytes(Publisher<byte[]> dataStream) {
    return source.sendBytes(dataStream);
  }

  @Override
  public AeronOutbound sendString(Publisher<String> dataStream) {
    return source.sendString(dataStream);
  }

  @Override
  public AeronOutbound sendBuffer(Publisher<ByteBuffer> dataStream) {
    return source.sendBuffer(dataStream);
  }

  @Override
  public Mono<Void> then() {
    return thenMono;
  }

  @Override
  public void dispose() {
    source.dispose();
  }

  @Override
  public boolean isDisposed() {
    return source.isDisposed();
  }
}

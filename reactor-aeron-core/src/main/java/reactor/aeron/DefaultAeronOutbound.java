package reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.Charset;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

final class DefaultAeronOutbound implements AeronOutbound {

  private final AeronWriteSequencer sequencer;
  private final MessagePublication publication;

  /**
   * Constructor.
   *
   * @param publication message publication
   */
  DefaultAeronOutbound(MessagePublication publication) {
    this.publication = publication;
    this.sequencer = new AeronWriteSequencer(publication);
  }

  @Override
  public AeronOutbound send(Publisher<? extends ByteBuf> dataStream) {
    return then(sequencer.write(dataStream));
  }

  @Override
  public AeronOutbound sendString(Publisher<String> dataStream) {
    return sendString(dataStream, Charset.defaultCharset(), ByteBufAllocator.DEFAULT);
  }

  @Override
  public AeronOutbound sendString(
      Publisher<String> source, Charset charset, ByteBufAllocator allocator) {
    return send(
        Flux.from(source)
            .map(
                s -> {
                  ByteBuf buffer = allocator.buffer();
                  buffer.writeCharSequence(s, charset);
                  return buffer;
                }));
  }

  void dispose() {
    publication.dispose();
  }
}

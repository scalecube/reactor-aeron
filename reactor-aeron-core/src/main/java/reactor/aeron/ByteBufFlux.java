package reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.Charset;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public final class ByteBufFlux extends FluxOperator<ByteBuf, ByteBuf> {

  public ByteBufFlux(Publisher<? extends ByteBuf> source) {
    this(Flux.from(source));
  }

  public ByteBufFlux(Flux<? extends ByteBuf> source) {
    super(source);
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> s) {
    source.subscribe(s);
  }

  /**
   * Convert to a {@link String} inbound {@link Flux} using the default {@link Charset}.
   *
   * @return a {@link String} inbound {@link Flux}
   */
  public Flux<String> asString() {
    return asString(Charset.defaultCharset());
  }

  /**
   * Convert to a {@link String} inbound {@link Flux} using the provided {@link Charset}.
   *
   * @param charset the decoding charset
   * @return a {@link String} inbound {@link Flux}
   */
  public final Flux<String> asString(Charset charset) {
    return map(bb -> bb.toString(charset));
  }

  /**
   * Decorate as {@link ByteBufFlux}.
   *
   * @param data array to decorate
   * @return a {@link ByteBufFlux}
   */
  public static ByteBufFlux fromString(String... data) {
    return new ByteBufFlux(
        Flux.fromArray(data)
            .map(
                s -> {
                  ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
                  buffer.writeCharSequence(s, Charset.defaultCharset());
                  return buffer;
                }));
  }
}

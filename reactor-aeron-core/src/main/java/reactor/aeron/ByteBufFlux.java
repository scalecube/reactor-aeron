package reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public final class ByteBufFlux extends FluxOperator<ByteBuf, ByteBuf> {

  private final ByteBufAllocator allocator;

  public ByteBufFlux(Publisher<? extends ByteBuf> source) {
    this(Flux.from(source), ByteBufAllocator.DEFAULT);
  }

  public ByteBufFlux(Publisher<? extends ByteBuf> source, ByteBufAllocator allocator) {
    this(Flux.from(source), allocator);
  }

  public ByteBufFlux(Flux<? extends ByteBuf> source) {
    this(source, ByteBufAllocator.DEFAULT);
  }

  public ByteBufFlux(Flux<? extends ByteBuf> source, ByteBufAllocator allocator) {
    super(source);
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> s) {
    source.subscribe(s);
  }

  /**
   * Disable auto memory release on each buffer published, retaining in order to prevent premature
   * recycling when buffers are accumulated downstream (async).
   *
   * @return {@link ByteBufFlux} of retained {@link ByteBuf}
   */
  public ByteBufFlux retain() {
    return new ByteBufFlux(doOnNext(ByteBuf::retain), allocator);
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
    return handle(
        (bb, sink) -> {
          try {
            sink.next(bb.readCharSequence(bb.readableBytes(), charset).toString());
          } catch (IllegalReferenceCountException e) {
            sink.complete();
          }
        });
  }

  /**
   * Convert to a {@link ByteBuffer} inbound {@link Flux}.
   *
   * @return a {@link ByteBuffer} inbound {@link Flux}
   */
  public final Flux<ByteBuffer> asByteBuffer() {
    return handle(
        (bb, sink) -> {
          try {
            sink.next(bb.nioBuffer());
          } catch (IllegalReferenceCountException e) {
            sink.complete();
          }
        });
  }

  /**
   * Convert to a {@literal byte[]} inbound {@link Flux}.
   *
   * @return a {@literal byte[]} inbound {@link Flux}
   */
  public final Flux<byte[]> asByteArray() {
    return handle(
        (bb, sink) -> {
          try {
            byte[] bytes = new byte[bb.readableBytes()];
            bb.readBytes(bytes);
            sink.next(bytes);
          } catch (IllegalReferenceCountException e) {
            sink.complete();
          }
        });
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

  /**
   * Decorate as {@link ByteBufFlux}.
   *
   * @param source publisher to decorate
   * @return a {@link ByteBufFlux}
   */
  public static ByteBufFlux fromString(Publisher<String> source) {
    return fromString(source, Charset.defaultCharset(), ByteBufAllocator.DEFAULT);
  }

  /**
   * Decorate as {@link ByteBufFlux}.
   *
   * @param source publisher to decorate
   * @param charset {@link Charset} instance
   * @param allocator {@link ByteBufAllocator} instance
   * @return a {@link ByteBufFlux}
   */
  public static ByteBufFlux fromString(
      Publisher<String> source, Charset charset, ByteBufAllocator allocator) {
    Objects.requireNonNull(allocator, "allocator");
    return new ByteBufFlux(
        Flux.from(source)
            .map(
                s -> {
                  ByteBuf buffer = allocator.buffer();
                  buffer.writeCharSequence(s, charset);
                  return buffer;
                }),
        allocator);
  }
}

package reactor.aeron;

import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public final class DirectBufferFlux extends FluxOperator<DirectBuffer, DirectBuffer> {

  public DirectBufferFlux(Flux<? extends DirectBuffer> source) {
    super(source);
  }

  @Override
  public void subscribe(CoreSubscriber<? super DirectBuffer> s) {
    source.subscribe(s);
  }

  /**
   * Applies transformation {@link DirectBuffer} to {@code String}.
   *
   * @return {@code Flux<String>} instance
   */
  public Flux<String> asString() {
    return map(
        buffer -> {
          byte[] bytes = new byte[buffer.capacity()];
          buffer.getBytes(0, bytes);
          return new String(bytes, StandardCharsets.UTF_8);
        });
  }
}

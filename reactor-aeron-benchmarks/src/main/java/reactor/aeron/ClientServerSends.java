package reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.Charset;
import org.agrona.DirectBuffer;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;

public class ClientServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources resources = new AeronResources().useTmpDir().start().block();

    AeronClient.create(resources)
        .options("localhost", 13000, 13001)
        .handle(
            connection ->
                connection
                    .inbound()
                    .receive()
                    .cast(DirectBuffer.class)
                    .as(ByteBufFlux::create)
                    .asString()
                    .log("receive")
                    .then(connection.onDispose()))
        .connect()
        .block()
        .onDispose(resources)
        .onDispose()
        .block();
  }

  static class ByteBufFlux extends FluxOperator<ByteBuf, ByteBuf> {

    public ByteBufFlux(Flux<? extends ByteBuf> source) {
      super(source);
    }

    public static ByteBufFlux create(Flux<DirectBuffer> directBufferFlux) {
      return new ByteBufFlux(
          directBufferFlux.map(
              buffer -> {
                byte[] bytes = new byte[buffer.capacity()];
                buffer.getBytes(0, bytes);
                return Unpooled.copiedBuffer(bytes);
              }));
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
      source.subscribe(actual);
    }

    public Flux<String> asString() {
      return map(buffer -> buffer.toString(Charset.defaultCharset()));
    }
  }
}

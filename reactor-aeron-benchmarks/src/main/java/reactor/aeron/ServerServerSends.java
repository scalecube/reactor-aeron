package reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.Charset;
import java.time.Duration;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Flux;

public class ServerServerSends {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    AeronResources resources = new AeronResources().useTmpDir().start().block();

    AeronServer.create(resources)
        .options("localhost", 13000, 13001)
        .handle(
            connection ->
                connection
                    .outbound()
                    .send(
                        Flux.range(1, 10000)
                            .delayElements(Duration.ofMillis(250))
                            .map(String::valueOf)
                            .log("send")
                            .map(s -> Unpooled.copiedBuffer(s, Charset.defaultCharset())),
                        ByteBufHandler.defaultInstance)
                    .then(connection.onDispose()))
        .bind()
        .block()
        .onDispose(resources)
        .onDispose()
        .block();
  }

  static class ByteBufHandler implements DirectBufferHandler<ByteBuf> {

    static final ByteBufHandler defaultInstance = new ByteBufHandler();

    @Override
    public int estimateLength(ByteBuf buffer) {
      return buffer.readableBytes();
    }

    @Override
    public DirectBuffer map(ByteBuf buffer, int length) {
      return new UnsafeBuffer(buffer.nioBuffer(), 0, length);
    }

    @Override
    public void dispose(ByteBuf buffer) {
      buffer.release();
    }
  }
}

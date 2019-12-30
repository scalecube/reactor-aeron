package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AeronOutbound extends Publisher<Void>, OnDisposable {

  /**
   * Send data to the peer, listen for any error on write and close on terminal signal
   * (complete|error).
   *
   * @param <B> abstract buffer type (comes from client code)
   * @param dataStream the dataStream publishing items to send
   * @param bufferHandler abstract buffer handler for {@link DirectBuffer} buffer
   * @return A new {@link AeronOutbound} to append further send. It will emit a complete signal upon
   *     successful sequence write or an error during write.
   */
  <B> AeronOutbound send(Publisher<B> dataStream, DirectBufferHandler<? super B> bufferHandler);

  /**
   * Send data to the peer, listen for any error on write and close on terminal signal
   * (complete|error).
   *
   * @param dataStream the dataStream publishing items to send
   * @return A new {@link AeronOutbound} to append further send. It will emit a complete signal upon
   *     successful sequence write or an error during write.
   */
  default AeronOutbound send(Publisher<DirectBuffer> dataStream) {
    return send(dataStream, buffer -> buffer);
  }

  /**
   * Send data to the peer, listen for any error on write and close on terminal signal
   * (complete|error).
   *
   * @param dataStream the dataStream publishing items to send
   * @return A new {@link AeronOutbound} to append further send. It will emit a complete signal upon
   *     successful sequence write or an error during write.
   */
  default AeronOutbound sendBytes(Publisher<byte[]> dataStream) {
    if (dataStream instanceof Flux) {
      return send(((Flux<byte[]>) dataStream).map(UnsafeBuffer::new));
    }
    return send(((Mono<byte[]>) dataStream).map(UnsafeBuffer::new));
  }

  /**
   * Send data to the peer, listen for any error on write and close on terminal signal
   * (complete|error).
   *
   * @param dataStream the dataStream publishing items to send
   * @return A new {@link AeronOutbound} to append further send. It will emit a complete signal upon
   *     successful sequence write or an error during write.
   */
  default AeronOutbound sendString(Publisher<String> dataStream) {
    if (dataStream instanceof Flux) {
      return send(
          ((Flux<String>) dataStream)
              .map(s -> s.getBytes(StandardCharsets.UTF_8))
              .map(UnsafeBuffer::new));
    }
    return send(
        ((Mono<String>) dataStream)
            .map(s -> s.getBytes(StandardCharsets.UTF_8))
            .map(UnsafeBuffer::new));
  }

  /**
   * Send data to the peer, listen for any error on write and close on terminal signal
   * (complete|error).
   *
   * @param dataStream the dataStream publishing items to send
   * @return A new {@link AeronOutbound} to append further send. It will emit a complete signal upon
   *     successful sequence write or an error during write.
   */
  default AeronOutbound sendBuffer(Publisher<ByteBuffer> dataStream) {
    if (dataStream instanceof Flux) {
      return send(((Flux<ByteBuffer>) dataStream).map(UnsafeBuffer::new));
    }
    return send(((Mono<ByteBuffer>) dataStream).map(UnsafeBuffer::new));
  }

  /**
   * Obtain a {@link Mono} of pending outbound(s) write completion.
   *
   * @return a {@link Mono} of pending outbound(s) write completion
   */
  default Mono<Void> then() {
    return Mono.empty();
  }

  /**
   * Append a {@link Publisher} task such as a Mono and return a new {@link AeronOutbound} to
   * sequence further send.
   *
   * @param other the {@link Publisher} to subscribe to when this pending outbound {@link #then} is
   *     complete;
   * @return a new {@link AeronOutbound}
   */
  default AeronOutbound then(Publisher<Void> other) {
    return new AeronOutboundThen(this, other);
  }

  /**
   * Subscribe a {@code Void} subscriber to this outbound and trigger all eventual parent outbound
   * send.
   *
   * @param s the {@link Subscriber} to listen for send sequence completion/failure
   */
  @Override
  default void subscribe(Subscriber<? super Void> s) {
    then().subscribe(s);
  }
}

package reactor.aeron.buffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Mono;

public class Test {

  public static void main(String[] args) throws InterruptedException {

    String msg = "hello";
    int length = msg.getBytes(StandardCharsets.UTF_8).length;

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);

    UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);

    BufferSlab allocator = new BufferSlab(unsafeBuffer);

    for (int i = 0; i < 100; i++) {

      Thread.sleep(100);

      BufferSlice slice = allocator.allocate(32 - 5);
      if (slice != null) {
        slice.putStringUtf8(0, msg);
        slice.print();
        System.out.println("---------- received msg: " + slice.getStringUtf8(0, length));

        Mono.delay(Duration.ofMillis(100)).doOnSuccess($ -> slice.release()).subscribe();
      }
    }
  }
}

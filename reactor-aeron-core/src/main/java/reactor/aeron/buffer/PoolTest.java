package reactor.aeron.buffer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import reactor.core.publisher.Mono;

public class PoolTest {

  public static void main(String[] args) throws InterruptedException {

    String msg = "hello";
    int length = msg.getBytes(StandardCharsets.UTF_8).length;

    BufferPool pool = new BufferPool(1024);

    for (int i = 0; i < 10000; i++) {
      Thread.sleep(1);
      BufferSlice slice = pool.allocate(233);
      slice.putStringUtf8(0, msg);
      Mono.delay(Duration.ofMillis(100)).doOnSuccess($ -> slice.release()).subscribe();
    }
  }
}

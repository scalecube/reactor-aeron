package reactor.aeron.buffer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import reactor.core.publisher.Mono;

public class PoolTest {

  public static void main(String[] args) throws InterruptedException {
    String msg = "hello";

    BufferPool pool = new BufferPool(10240);

    Random random = new Random();

    for (int i = 0; i < 1000000; i++) {
      Thread.sleep(50);
      BufferSlice slice = pool.allocate(23 + random.nextInt(500));
      slice.putStringUtf8(0, msg);
//      slice.print();
      Mono.delay(Duration.ofMillis(20)).doOnSuccess($ -> slice.release()).subscribe();
    }
  }
}

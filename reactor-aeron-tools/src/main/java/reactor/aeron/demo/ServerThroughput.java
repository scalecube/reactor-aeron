package reactor.aeron.demo;

import io.netty.util.ReferenceCountUtil;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.core.scheduler.Schedulers;

public class ServerThroughput {

  static final int SLIDING_AVG_DURATION_SEC = 5;

  static {
    System.setProperty("io.netty.leakDetection.level", "PARANOID");
  }

  static class Data {

    private final long time;

    private final int size;

    Data(long time, int size) {
      this.time = time;
      this.size = size;
    }
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    AeronResources aeronResources = AeronResources.start();
    try {
      Queue<Data> queue = new ConcurrentLinkedDeque<>();
      AtomicLong counter = new AtomicLong();

      Schedulers.single()
          .schedulePeriodically(
              () -> {
                long end = now();
                long cutoffTime = end - TimeUnit.SECONDS.toMillis(SLIDING_AVG_DURATION_SEC);

                long value = counter.getAndSet(0);
                long total = 0;
                Iterator<Data> it = queue.iterator();
                while (it.hasNext()) {
                  Data data = it.next();
                  if (data.time < cutoffTime) {
                    it.remove();
                  } else {
                    total += data.size;
                  }
                }

                System.out.printf(
                    "Rate: %d MB/s, %ds avg rate: %d MB/s\n",
                    toMb(value), SLIDING_AVG_DURATION_SEC, toMb(total) / SLIDING_AVG_DURATION_SEC);
              },
              1,
              1,
              TimeUnit.SECONDS);

      AeronServer.create(aeronResources)
          .options("localhost", 13000, 13001)
          .handle(
              connection ->
                  connection
                      .inbound()
                      .receive()
                      .doOnNext(
                          buffer -> {
                            int size = buffer.readableBytes();
                            System.err.println(size);
                            queue.add(new Data(now(), size));
                            counter.addAndGet(size);
                            ReferenceCountUtil.safeRelease(buffer);
                          })
                      .then(connection.onDispose()))
          .bind()
          .block();

      Thread.currentThread().join();
    } finally {
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static long toMb(long value) {
    return value / (1024 * 1024);
  }
}

package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import reactor.aeron.AeronResources;

public final class SingleMediaDriver {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    new AeronResources()
        .numOfWorkers(0)
        .useTmpDir()
        .media(ctx -> ctx.threadingMode(ThreadingMode.DEDICATED))
        .start()
        .block()
        .onDispose()
        .block();
  }
}

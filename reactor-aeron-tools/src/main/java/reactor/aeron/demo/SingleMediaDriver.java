package reactor.aeron.demo;

import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BusySpinIdleStrategy;
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
        .media(ctx -> ctx.threadingMode(ThreadingMode.SHARED)
            .receiverIdleStrategy(new BusySpinIdleStrategy())
            .senderIdleStrategy(new BusySpinIdleStrategy())
            .conductorIdleStrategy(new BusySpinIdleStrategy()))
        .start()
        .block()
        .onDispose()
        .block();
  }
}

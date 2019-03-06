package reactor.aeron.rsocket.aeron;

import io.aeron.driver.Configuration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.reactor.aeron.AeronServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Random;
import java.util.concurrent.Callable;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.aeron.Configurations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class RSocketAeronServerTps {

  private static final ByteBuf BUFFER =
      ByteBufAllocator.DEFAULT.buffer(Configurations.MESSAGE_LENGTH);

  static {
    Random random = new Random(System.nanoTime());
    byte[] bytes = new byte[Configurations.MESSAGE_LENGTH];
    random.nextBytes(bytes);
    BUFFER.writeBytes(bytes);
  }

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {

    printSettings();

    AeronResources resources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(Configurations.FRAGMENT_COUNT_LIMIT)
            .singleWorker()
            .workerIdleStrategySupplier(Configurations::idleStrategy)
            .start()
            .block();

    RSocketFactory.receive()
        .frameDecoder(Frame::retain)
        .acceptor(
            (setupPayload, rsocket) ->
                Mono.just(
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        payload.release();

                        Callable<Payload> payloadCallable =
                            () -> ByteBufPayload.create(BUFFER.retainedSlice());

                        return Mono.fromCallable(payloadCallable)
                            .subscribeOn(Schedulers.parallel())
                            .repeat(Configurations.NUMBER_OF_MESSAGES);
                      }
                    }))
        .transport(
            new AeronServerTransport(
                AeronServer.create(resources)
                    .options(
                        Configurations.MDC_ADDRESS,
                        Configurations.MDC_PORT,
                        Configurations.MDC_CONTROL_PORT)))
        .start()
        .block()
        .onClose()
        .doFinally(s -> resources.dispose())
        .then(resources.onDispose())
        .block();
  }

  private static void printSettings() {
    System.out.println(
        "address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT
            + ", controlPort: "
            + Configurations.MDC_CONTROL_PORT);
    System.out.println("MediaDriver THREADING_MODE: " + Configuration.THREADING_MODE_DEFAULT);
    System.out.println("Message length of " + Configurations.MESSAGE_LENGTH + " bytes");
    System.out.println("pollFragmentLimit of " + Configurations.FRAGMENT_COUNT_LIMIT);
    System.out.println(
        "Using worker idle strategy "
            + Configurations.idleStrategy().getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");
  }
}

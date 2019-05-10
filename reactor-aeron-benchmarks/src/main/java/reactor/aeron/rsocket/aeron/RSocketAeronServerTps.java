package reactor.aeron.rsocket.aeron;

import io.aeron.driver.Configuration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.frame.decoder.ZeroCopyPayloadDecoder;
import io.rsocket.reactor.aeron.AeronServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Random;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronServer;
import reactor.aeron.Configurations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
        .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor(
            (setupPayload, rsocket) ->
                Mono.just(
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        payload.release();

                        long msgNum = Configurations.NUMBER_OF_MESSAGES;
                        System.out.println("streaming " + msgNum + " messages ...");

                        return Flux.range(0, Integer.MAX_VALUE)
                            .map(i -> ByteBufPayload.create(BUFFER.retainedSlice()));
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

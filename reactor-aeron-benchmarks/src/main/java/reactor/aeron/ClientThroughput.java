package reactor.aeron;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.mdc.AeronClient;
import reactor.aeron.mdc.AeronResources;
import reactor.core.publisher.Flux;

public class ClientThroughput {

  private static final UnsafeBuffer OFFER_BUFFER =
      new UnsafeBuffer(
          BufferUtil.allocateDirectAligned(
              Configurations.MESSAGE_LENGTH, BitUtil.CACHE_LINE_LENGTH));

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {

    System.out.println(
        "address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT
            + ", controlPort: "
            + Configurations.MDC_CONTROL_PORT);
    System.out.println("Message length of " + Configurations.MESSAGE_LENGTH + " bytes");
    System.out.println("pollFragmentLimit of " + Configurations.FRAGMENT_COUNT_LIMIT);
    System.out.println(
        "Using worker idle strategy "
            + Configurations.idleStrategy().getClass()
            + "("
            + Configurations.IDLE_STRATEGY
            + ")");

    AeronResources aeronResources =
        new AeronResources()
            .useTmpDir()
            .pollFragmentLimit(Configurations.FRAGMENT_COUNT_LIMIT)
            .singleWorker()
            .workerIdleStrategySupplier(Configurations::idleStrategy)
            .start()
            .block();

    AeronClient.create(aeronResources)
        .options(
            Configurations.MDC_ADDRESS, Configurations.MDC_PORT, Configurations.MDC_CONTROL_PORT)
        .handle(
            connection ->
                connection
                    .outbound()
                    .send(
                        Flux.range(0, Byte.MAX_VALUE)
                            .repeat(Integer.MAX_VALUE)
                            .map(i -> OFFER_BUFFER))
                    .then(connection.onDispose()))
        .connect()
        .block()
        .onDispose()
        .doFinally(s -> aeronResources.dispose())
        .then(aeronResources.onDispose())
        .block();
  }
}

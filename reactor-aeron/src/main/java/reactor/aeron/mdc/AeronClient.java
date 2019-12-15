package reactor.aeron.mdc;

import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.agrona.DirectBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public final class AeronClient {

  private final AeronOptions options;

  private AeronClient(AeronOptions options) {
    this.options = options;
  }

  /**
   * Creates {@link AeronClient}.
   *
   * @param resources aeron resources
   * @return new {@code AeronClient}
   */
  public static AeronClient create(AeronResources resources) {
    return new AeronClient(new AeronOptions().resources(resources));
  }

  /**
   * Connects {@link AeronClient}.
   *
   * @return mono handle of result
   */
  public Mono<? extends AeronConnection<DirectBuffer>> connect() {
    return connect(s -> s);
  }

  /**
   * Connects {@link AeronClient} with options.
   *
   * @param op unary opearator for performing setup of options
   * @return mono handle of result
   */
  public Mono<? extends AeronConnection<DirectBuffer>> connect(UnaryOperator<AeronOptions> op) {
    return Mono.defer(() -> new AeronClientConnector(op.apply(options)).start());
  }

  /**
   * Setting up {@link AeronClient} options.
   *
   * @param op unary opearator for performing setup of options
   * @return new {@code AeronClient} with applied options
   */
  public AeronClient options(UnaryOperator<AeronOptions> op) {
    return new AeronClient(op.apply(options));
  }

  /**
   * Shortcut client settings.
   *
   * @param address server address
   * @param port server port
   * @param controlPort server control port
   * @return new {@code AeronClient} with applied options
   */
  public AeronClient options(String address, int port, int controlPort) {
    return new AeronClient(options)
        .options(
            opts -> {
              String endpoint = address + ':' + port;
              String controlEndpoint = address + ':' + controlPort;

              AeronChannelUriString inboundUri =
                  opts.inboundUri()
                      .uri(b -> b.controlEndpoint(controlEndpoint).controlMode("dynamic"));

              AeronChannelUriString outboundUri = opts.outboundUri().uri(b -> b.endpoint(endpoint));

              return opts //
                  .outboundUri(outboundUri) // Pub
                  .inboundUri(inboundUri); // Sub->MDC(sessionId)
            });
  }

  /**
   * Attach IO handler to react on connected client.
   *
   * @param handler IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return new {@code AeronClient} with handler
   */
  public AeronClient handle(
      Function<? super AeronConnection<DirectBuffer>, ? extends Publisher<Void>> handler) {
    return new AeronClient(options.handler(handler));
  }
}

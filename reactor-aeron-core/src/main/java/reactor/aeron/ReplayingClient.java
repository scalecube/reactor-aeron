package reactor.aeron;

import java.util.function.UnaryOperator;
import reactor.core.publisher.Mono;

public class ReplayingClient extends AeronClient {


  private ReplayingClient(AeronOptions options) {
    super(options);
  }

  @Override
  public Mono<? extends AeronConnection> connect(UnaryOperator<AeronOptions> op) {
    return Mono.defer(() -> new ReplayingClientConnector(op.apply(options)).start());
  }

}

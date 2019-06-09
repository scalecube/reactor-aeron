package reactor.aeron;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public interface AeronInbound<T> extends Disposable {

  Flux<T> receive();
}

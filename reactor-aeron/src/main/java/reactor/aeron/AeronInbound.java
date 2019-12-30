package reactor.aeron;

import reactor.core.publisher.Flux;

public interface AeronInbound<T> extends OnDisposable {

  Flux<T> receive();
}

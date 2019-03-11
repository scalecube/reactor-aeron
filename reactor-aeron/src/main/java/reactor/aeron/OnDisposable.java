package reactor.aeron;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface OnDisposable extends Disposable {

  /**
   * Assign a {@link Disposable} to be invoked when the this has been disposed.
   *
   * @return a Mono succeeding when this has been disposed
   */
  Mono<Void> onDispose();

  /**
   * Assign a {@link Disposable} to be invoked when the connection is closed.
   *
   * @param onDispose the close event handler
   * @return this
   */
  default OnDisposable onDispose(Disposable onDispose) {
    onDispose().subscribe(null, e -> onDispose.dispose(), onDispose::dispose);
    return this;
  }
}

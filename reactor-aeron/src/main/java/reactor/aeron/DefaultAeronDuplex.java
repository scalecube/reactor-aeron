package reactor.aeron;

import reactor.core.publisher.Mono;

public class DefaultAeronDuplex<I> implements AeronDuplex<I> {

  private final AeronInbound<I> inbound;
  private final AeronOutbound outbound;

  /**
   * Constructor.
   *
   * @param inbound inbound
   * @param outbound outbound
   */
  public DefaultAeronDuplex(AeronInbound<I> inbound, AeronOutbound outbound) {
    this.inbound = inbound;
    this.outbound = outbound;
    inbound.onDispose(this);
    outbound.onDispose(this);
  }

  @Override
  public AeronInbound<I> inbound() {
    return inbound;
  }

  @Override
  public AeronOutbound outbound() {
    return outbound;
  }

  @Override
  public Mono<Void> onDispose() {
    return Mono.whenDelayError(inbound.onDispose(), outbound.onDispose());
  }

  @Override
  public void dispose() {
    inbound.dispose();
    outbound.dispose();
  }

  @Override
  public boolean isDisposed() {
    return inbound.isDisposed() && outbound.isDisposed();
  }
}

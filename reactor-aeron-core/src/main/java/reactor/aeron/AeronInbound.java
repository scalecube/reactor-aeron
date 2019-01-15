package reactor.aeron;

public interface AeronInbound {

  ByteBufFlux receive();
}

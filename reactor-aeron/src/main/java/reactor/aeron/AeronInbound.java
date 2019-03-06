package reactor.aeron;

public interface AeronInbound {

  DirectBufferFlux receive();
}

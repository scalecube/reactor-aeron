package rsocket.demo;

import io.rsocket.AbstractRSocket;
import io.rsocket.Frame;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpServer;

public final class TcpPongServer {

  public static void main(String... args) {

    TcpServer tcpServer =
        TcpServer.create()
            .runOn(LoopResources.create("server", 1, true))
            .host("localhost")
            .port(7878);


    RSocketFactory.receive()
//        .frameDecoder(Frame::retain)


        .frameDecoder(
            frame ->
                ByteBufPayload.create(
                    frame.sliceData().retain(), frame.sliceMetadata().retain()))

        .acceptor(
            (setupPayload, reactiveSocket) ->
                Mono.just(
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return Flux.from(payloads)
//                            .doOnNext(Payload::retain)
//                            .doOnNext(
//                                p -> {
//                                  System.out.println(p.getData().getLong());
//                                })
                            .map(DefaultPayload::create)
                            ;
                      }
                    }))
        .transport(() -> TcpServerTransport.create(tcpServer))
        .start()
        .block()
        .onClose()
        .block();
  }
}

package reactor.aeron.rsocket.netty;

import io.netty.channel.ChannelOption;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import reactor.aeron.Configurations;
import reactor.aeron.RateReporter;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

public final class RSocketNettyClientTps {

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String... args) {
    System.out.println(
        "message size: "
            + Configurations.MESSAGE_LENGTH
            + ", number of messages: "
            + Configurations.NUMBER_OF_MESSAGES
            + ", address: "
            + Configurations.MDC_ADDRESS
            + ", port: "
            + Configurations.MDC_PORT);

    RateReporter reporter = new RateReporter();

    LoopResources loopResources = LoopResources.create("rsocket-netty");

    TcpClient tcpClient =
        TcpClient.create(ConnectionProvider.newConnection())
            .runOn(loopResources)
            .host(Configurations.MDC_ADDRESS)
            .port(Configurations.MDC_PORT)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .doOnConnected(System.out::println);

    RSocket client =
        RSocketFactory.connect()
            //.frameDecoder(Frame::retain)
            .transport(() -> TcpClientTransport.create(tcpClient))
            .start()
            .doOnSuccess(System.out::println)
            .block();

    Payload request = ByteBufPayload.create("hello");

    client
        .requestStream(request)
        .doOnNext(
            payload -> {
              reporter.onMessage(1, payload.sliceData().readableBytes());
              payload.release();
            })
        .doOnError(Throwable::printStackTrace)
        .doFinally(s -> reporter.dispose())
        .then()
        .block();
  }
}

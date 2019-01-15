package reactor.aeron;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

class AeronServerTest extends BaseAeronTest {

  private int serverPort;
  private int serverControlPort;
  private AeronResources clientResources;
  private AeronResources serverResources;

  @BeforeEach
  void beforeEach() {
    serverPort = SocketUtils.findAvailableUdpPort();
    serverControlPort = SocketUtils.findAvailableUdpPort();
    clientResources = AeronResources.start(AeronResourcesConfig.builder().numOfWorkers(1).build());
    serverResources = AeronResources.start(AeronResourcesConfig.builder().numOfWorkers(1).build());
  }

  @AfterEach
  void afterEach() {
    if (clientResources != null) {
      clientResources.dispose();
      clientResources.onDispose().block(TIMEOUT);
    }
    if (serverResources != null) {
      serverResources.dispose();
      serverResources.onDispose().block(TIMEOUT);
    }
  }

  @Test
  public void testServerReceivesData() {
    ReplayProcessor<String> processor = ReplayProcessor.create();

    createServer(
        connection -> {
          connection.inbound().receive().asString().log("receive").subscribe(processor);
          return connection.onDispose();
        });

    createConnection()
        .outbound()
        .send(ByteBufFlux.fromString("Hello", "world!").log("send"))
        .then()
        .subscribe();

    StepVerifier.create(processor).expectNext("Hello", "world!").thenCancel().verify();
  }

  @Test
  public void testServerDisconnectsClientsUponShutdown() throws InterruptedException {
    ReplayProcessor<ByteBuf> processor = ReplayProcessor.create();

    OnDisposable server =
        createServer(
            connection -> {
              connection.inbound().receive().log("receive").subscribe(processor);
              return connection.onDispose();
            });

    createConnection()
        .outbound()
        .sendString(
            Flux.range(1, 100)
                .delayElements(Duration.ofSeconds(1))
                .map(String::valueOf)
                .log("send"))
        .then()
        .subscribe();

    processor.blockFirst();

    server.dispose();

    // TODO add StepVerifier here and remove ThreadWatcher class

    ThreadWatcher threadWatcher = new ThreadWatcher();

    assertTrue(threadWatcher.awaitTerminated(5000, "single-", "parallel-"));
  }

  private AeronConnection createConnection() {
    return AeronClient.create(clientResources)
        .options("localhost", serverPort, serverControlPort)
        .connect()
        .block(TIMEOUT);
  }

  private OnDisposable createServer(
      Function<? super AeronConnection, ? extends Publisher<Void>> handler) {
    return AeronServer.create(serverResources)
        .options("localhost", serverPort, serverControlPort)
        .handle(handler)
        .bind()
        .block(TIMEOUT);
  }
}

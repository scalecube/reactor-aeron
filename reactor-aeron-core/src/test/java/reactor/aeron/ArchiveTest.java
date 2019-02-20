package reactor.aeron;

import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static reactor.aeron.BaseAeronTest.TIMEOUT;

import io.aeron.ChannelUriStringBuilder;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;
import reactor.test.StepVerifier;

public class ArchiveTest {

  private int serverPort;
  private int serverControlPort;
  private AeronResources resources;

  private static final int TERM_BUFFER_LENGTH = 64 * 1024;
  private static final int RECORDING_STREAM_ID = 33;
  private static final String RECORDING_CHANNEL = new ChannelUriStringBuilder()
      .media("udp")
      .endpoint("localhost:3333")
      .termLength(TERM_BUFFER_LENGTH)
      .build();

  private static final int REPLAY_STREAM_ID = 66;
  private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
      .media("udp")
      .endpoint("localhost:6666")
      .build();

  @BeforeEach
  void beforeEach() {
    serverPort = SocketUtils.findAvailableUdpPort();
    serverControlPort = SocketUtils.findAvailableUdpPort();
    resources = new AeronResources().useTmpDir().singleWorker().start().block();
  }

  @AfterEach
  void afterEach() {
    if (resources != null) {
      resources.dispose();
      resources.onDispose().block(TIMEOUT);
    }
  }

  @Test
  void testRecordThenReplay() {

    final String messagePrefix = "Prefix-";
    final int messageCount = 10;
    Flux<String> payload = Flux
        .fromStream(IntStream.range(0, messageCount).mapToObj(i -> messagePrefix + i));
    final long stopPosition;

    // create "logging" server
    ReplayProcessor<String> subscriptionProcessor = ReplayProcessor.create();

    OnDisposable server = AeronServer.create(resources)
        .options("localhost", serverPort, serverControlPort)
        .handle(connection -> {

          connection.archive().startRecording()

          connection.inbound().receive().asString().log("receive").subscribe(subscriptionProcessor);
          return connection.onDispose();
        })
        .bind()
        .block(TIMEOUT);

    AeronConnection connection = createConnection();


  }

  @Test
  public void testServerReceivesData() {
    ReplayProcessor<String> subscriptionProcessor = ReplayProcessor.create();

    OnDisposable server = createServer(
        connection -> {
          connection.inbound().receive().asString().log("receive").subscribe(subscriptionProcessor);
          return connection.onDispose();
        });

    AeronConnection connection = createConnection();

    connection
        .outbound()
        .sendString(Flux.fromStream(Stream.of("Hello", "world!")).log("send"))
        .then()
        .subscribe();

    StepVerifier.create(subscriptionProcessor).expectNext("Hello", "world!").thenCancel().verify();
  }

  private AeronConnection createConnection() {
    return AeronClient.create(resources)
        .options("localhost", serverPort, serverControlPort)
        .connect()
        .block(TIMEOUT);
  }

  private OnDisposable createServer(
      Function<? super AeronConnection, ? extends Publisher<Void>> handler) {
    return AeronServer.create(resources)
        .options("localhost", serverPort, serverControlPort)
        .handle(handler)
        .bind()
        .block(TIMEOUT);
  }

}

package reactor.aeron;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class AeronMultiClientTest extends BaseAeronTest {

  private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(1000);
  private static final int CONNECT_RETRY_COUNT = 5;

  private int serverPort;
  private int serverControlPort;
  private AeronResources serverResources;
  private List<AeronResources> clientResources = new ArrayList<>();

  @BeforeEach
  void beforeEach() {
    serverPort = SocketUtils.findAvailableUdpPort();
    serverControlPort = SocketUtils.findAvailableUdpPort();
    serverResources = new AeronResources().useTmpDir().singleWorker().start().block();
  }

  @AfterEach
  void afterEach() {
    Mono.whenDelayError(
            clientResources.stream()
                .peek(AeronResources::dispose)
                .map(AeronResources::onDispose)
                .collect(Collectors.toList()))
        .block(TIMEOUT);
    if (serverResources != null) {
      serverResources.dispose();
      serverResources.onDispose().block(TIMEOUT);
    }
  }

  @Test
  public void testMultiClientWithTheSameSessionId() {
    int sessionId = 100500;

    OnDisposable server =
        AeronServer.create(serverResources)
            .options("localhost", serverPort, serverControlPort)
            .bind()
            .block(TIMEOUT);

    // create the first client connection
    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(
                    options ->
                        options.outboundUri(options.outboundUri().uri(o -> o.sessionId(sessionId))))
                .connect())
        .expectNextCount(1)
        .expectComplete()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));

    // create the second client connection with the same session id
    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(
                    options ->
                        options.outboundUri(options.outboundUri().uri(o -> o.sessionId(sessionId))))
                .connect())
        .expectError()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));
  }

  @Test
  public void testMultiClientWithTheSameSessionIdWithRecovery() {
    OnDisposable server =
        AeronServer.create(serverResources)
            .options("localhost", serverPort, serverControlPort)
            .bind()
            .block(TIMEOUT);

    // create the first client connection
    AtomicInteger sessionIdCounterClient1 = new AtomicInteger();
    Supplier<Integer> sessionIdGeneratorClient1 = sessionIdCounterClient1::getAndIncrement;

    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(options -> options.connectRetryCount(0))
                .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient1))
                .connect())
        .expectNextCount(1)
        .expectComplete()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));

    // create the second client connection
    AtomicInteger sessionIdCounterClient2 = new AtomicInteger();
    Supplier<Integer> sessionIdGeneratorClient2 = sessionIdCounterClient2::getAndIncrement;

    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(options -> options.connectRetryCount(1))
                .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient2))
                .connect())
        .expectNextCount(1)
        .expectComplete()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));

    // create the third client connection
    AtomicInteger sessionIdCounterClient3 = new AtomicInteger();
    Supplier<Integer> sessionIdGeneratorClient3 = sessionIdCounterClient3::getAndIncrement;

    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(options -> options.connectRetryCount(2))
                .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient3))
                .connect())
        .expectNextCount(1)
        .expectComplete()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));
  }

  @Test
  public void testMultiClientReachRetryLimit() {
    int maxAvailableRetryCount = 2;

    OnDisposable server =
        AeronServer.create(serverResources)
            .options("localhost", serverPort, serverControlPort)
            .bind()
            .block(TIMEOUT);

    // create the first client connection
    AtomicInteger sessionIdCounterClient1 = new AtomicInteger();
    Supplier<Integer> sessionIdGeneratorClient1 = sessionIdCounterClient1::incrementAndGet;

    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(options -> options.connectRetryCount(0))
                .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient1))
                .connect())
        .expectNextCount(1)
        .expectComplete()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));

    // create the second client connection and sessionIdGenerator always gives the engaged session
    // id
    Supplier<Integer> sessionIdGeneratorClient2 = sessionIdCounterClient1::get;

    StepVerifier.create(
            AeronClient.create(newClientResources())
                .options("localhost", serverPort, serverControlPort)
                .options(options -> options.connectTimeout(CONNECT_TIMEOUT))
                .options(options -> options.connectRetryCount(maxAvailableRetryCount))
                .options(options -> options.sessionIdGenerator(sessionIdGeneratorClient2))
                .connect())
        .expectError()
        .verify(CONNECT_TIMEOUT.multipliedBy(CONNECT_RETRY_COUNT));
  }

  private AeronResources newClientResources() {
    AeronResources resources = new AeronResources().useTmpDir().singleWorker().start().block();
    clientResources.add(resources);
    return resources;
  }
}

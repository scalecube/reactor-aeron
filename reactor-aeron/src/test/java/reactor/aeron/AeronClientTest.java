package reactor.aeron;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.aeron.driver.Configuration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

class AeronClientTest extends BaseAeronTest {

  private static final Logger logger = LoggerFactory.getLogger(AeronClientTest.class);

  private int serverPort;
  private int serverControlPort;
  private AeronResources resources;

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
  public void testClientReceivesDataFromServer() {
    createServer(
        connection ->
            connection
                .outbound()
                .sendString(Flux.fromStream(Stream.of("hello1", "2", "3")).log("server"))
                .then(connection.onDispose()));

    AeronConnection connection = createConnection();
    StepVerifier.create(connection.inbound().receive().asString().log("client"))
        .expectNext("hello1", "2", "3")
        .expectNoEvent(Duration.ofMillis(10))
        .thenCancel()
        .verify();
  }

  @Test
  public void testClientReceivesLongDataFromServer() {
    char[] chars = new char[Configuration.mtuLength() * 2];
    Arrays.fill(chars, 'a');
    String str = new String(chars);

    createServer(
        connection ->
            connection
                .outbound()
                .sendString(Flux.fromStream(Stream.of(str, str, str)).log("server"))
                .then(connection.onDispose()));

    AeronConnection connection = createConnection();

    StepVerifier.create(connection.inbound().receive().asString().log("client"))
        .expectNext(str, str, str)
        .expectNoEvent(Duration.ofMillis(10))
        .thenCancel()
        .verify();
  }

  @Test
  public void testTwoClientsReceiveDataFromServer() {
    createServer(
        connection ->
            connection
                .outbound()
                .sendString(Flux.fromStream(Stream.of("1", "2", "3")).log("server"))
                .then(connection.onDispose()));

    AeronConnection connection1 = createConnection();
    AeronConnection connection2 = createConnection();

    StepVerifier.create(connection1.inbound().receive().asString().log("client-1"))
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();

    StepVerifier.create(connection2.inbound().receive().asString().log("client-2"))
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testClientsReceiveDataFromServer200000() {
    int count = 200_000;
    Flux<String> payloads = Flux.range(0, count).map(String::valueOf);

    createServer(
        connection -> connection.outbound().sendString(payloads).then(connection.onDispose()));

    AeronConnection connection1 = createConnection();

    StepVerifier.create(connection1.inbound().receive().asString())
        .expectNextCount(count)
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testRequestResponse200000() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .outbound()
                .send(connection.inbound().receive())
                .then(connection.onDispose()));

    AeronConnection connection1 = createConnection();

    connection1.outbound().sendString(Flux.range(0, count).map(String::valueOf)).then().subscribe();

    StepVerifier.create(connection1.inbound().receive().asString())
        .expectNextCount(count)
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testRequestResponse200000MonoJust() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .flatMap(byteBuffer -> connection.outbound().send(Mono.just(byteBuffer)).then())
                .then(connection.onDispose()));

    AeronConnection connection1 = createConnection();

    Flux.range(0, count)
        .flatMap(
            i ->
                connection1
                    .outbound()
                    .sendString(Flux.fromStream(Stream.of("client_send:" + i)))
                    .then())
        .then()
        .subscribe();

    StepVerifier.create(connection1.inbound().receive().asString())
        .expectNextCount(count)
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify(Duration.ofSeconds(20));
  }

  @Test
  public void testTwoClientsRequestResponse200000() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .flatMap(byteBuffer -> connection.outbound().send(Mono.just(byteBuffer)).then())
                .then(connection.onDispose()));

    ReplayProcessor<String> processor1 = ReplayProcessor.create();
    ReplayProcessor<String> processor2 = ReplayProcessor.create();

    createConnection(
        connection -> {
          connection.inbound().receive().asString().subscribe(processor1);
          Flux.range(0, count)
              .flatMap(
                  i -> connection.outbound().sendString(Mono.just("client-1 send:" + i)).then())
              .then()
              .subscribe();
          return connection.onDispose();
        });

    createConnection(
        connection -> {
          connection.inbound().receive().asString().subscribe(processor2);
          Flux.range(0, count)
              .flatMap(
                  i -> connection.outbound().sendString(Mono.just("client-2 send:" + i)).then())
              .then()
              .subscribe();
          return connection.onDispose();
        });

    StepVerifier.create(
            Flux.merge(
                processor1.take(count).filter(response -> !response.startsWith("client-1 ")),
                processor2.take(count).filter(response -> !response.startsWith("client-2 "))))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testTwoClientsRequestResponseWithDelaySubscriptionToInbound200000() {
    int count = 200_000;
    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .flatMap(byteBuffer -> connection.outbound().send(Mono.just(byteBuffer)).then())
                .then(connection.onDispose()));

    AeronConnection connection1 = createConnection();
    Flux.range(0, count)
        .flatMap(i -> connection1.outbound().sendString(Mono.just("client-1 send:" + i)).then())
        .then()
        .subscribe(null, ex -> logger.error("client-1 didn't send all, cause: ", ex));

    AeronConnection connection2 = createConnection();
    Flux.range(0, count)
        .flatMap(i -> connection2.outbound().sendString(Mono.just("client-2 send:" + i)).then())
        .then()
        .subscribe(null, ex -> logger.error("client-2 didn't send all, cause: ", ex));

    StepVerifier.create(
            Mono.delay(Duration.ofMillis(100))
                .thenMany(
                    Flux.merge(
                        connection1
                            .inbound()
                            .receive()
                            .asString()
                            .take(count)
                            .filter(response -> !response.startsWith("client-1 ")),
                        connection2
                            .inbound()
                            .receive()
                            .asString()
                            .take(count)
                            .filter(response -> !response.startsWith("client-2 ")))))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testClientWith2HandlersReceiveData() {
    createServer(
        connection ->
            connection
                .outbound()
                .sendString(Flux.fromStream(Stream.of("1", "2", "3")).log("server"))
                .then(connection.onDispose()));

    ReplayProcessor<String> processor1 = ReplayProcessor.create();
    ReplayProcessor<String> processor2 = ReplayProcessor.create();

    createConnection(
        connection -> {
          connection.inbound().receive().asString().log("client-1").subscribe(processor1);
          return connection.onDispose();
        });

    createConnection(
        connection -> {
          connection.inbound().receive().asString().log("client-2").subscribe(processor2);
          return connection.onDispose();
        });

    StepVerifier.create(processor1)
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();

    StepVerifier.create(processor2)
        .expectNext("1", "2", "3")
        .expectNoEvent(Duration.ofMillis(100))
        .thenCancel()
        .verify();
  }

  @Test
  public void testConcurrentSendingStreams() {
    int streams = 4;
    int overallCount = Queues.SMALL_BUFFER_SIZE * streams * 2;
    int requestPerStream = overallCount / streams;

    ReplayProcessor<String> clientRequests = ReplayProcessor.create();

    createServer(
        connection ->
            connection
                .inbound()
                .receive()
                .asString()
                .doOnNext(clientRequests::onNext)
                // .log("server receive ")
                .then(connection.onDispose()));

    createConnection(
        connection -> {
          for (int i = 0; i < streams; i++) {
            int start = i * requestPerStream;
            int count = start + requestPerStream;
            connection
                .outbound()
                .sendString(Flux.range(start, count).map(String::valueOf))
                .then()
                .subscribe(null, Throwable::printStackTrace);
          }
          return connection.onDispose();
        });

    List<Integer> requests =
        clientRequests
            .take(requestPerStream * streams)
            .map(Integer::parseInt)
            .collectList()
            .block(Duration.ofSeconds(10));

    //noinspection ConstantConditions
    ArrayList<Integer> sortedRequests = new ArrayList<>(requests);
    Collections.sort(sortedRequests);
    assertNotEquals(requests, sortedRequests);
  }

  @Test
  public void testCustomClientInboundSubscriber() {
    int count = 1000;
    Duration timeout = Duration.ofSeconds(5);

    int request1 = count * 10 / 100; // 10%
    int request2 = request1 * 2; // 20%
    int request3 = request1 * 7; // 70%
    int overall = request1 + request2 + request3; // 100%

    Scheduler scheduler = Schedulers.single();

    createServer(
        connection -> {
          connection
              .outbound()
              .sendString(Flux.range(0, overall).map(i -> "server send:" + i))
              .then()
              .subscribe();
          return connection.onDispose();
        });

    AeronConnection connection1 = createConnection();

    BaseSubscriber<String> subscriber =
        new BaseSubscriber<String>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            // no-op
          }
        };

    Duration smallInterval = timeout.multipliedBy(30).dividedBy(100); // 30%
    long requestDelay1 = smallInterval.dividedBy(3).toMillis(); // 10%
    long requestDelay2 = requestDelay1 + smallInterval.toMillis(); // 40%
    long requestDelay3 = requestDelay2 + smallInterval.toMillis(); // 70%

    scheduler.schedule(() -> subscriber.request(request1), requestDelay1, TimeUnit.MILLISECONDS);
    scheduler.schedule(() -> subscriber.request(request2), requestDelay2, TimeUnit.MILLISECONDS);
    scheduler.schedule(() -> subscriber.request(request3), requestDelay3, TimeUnit.MILLISECONDS);

    DirectProcessor<String> processor = DirectProcessor.create();
    connection1
        .inbound()
        .receive()
        .asString()
        .take(overall)
        .doOnNext(processor::onNext)
        .subscribe(subscriber);

    StepVerifier.create(processor.buffer(smallInterval).map(List::size))
        .expectNext(request1)
        .expectNext(request2)
        .expectNext(request3)
        .expectNoEvent(smallInterval)
        .thenCancel()
        .verify(timeout);
  }

  @Test
  public void testCustomClientInboundSubscriberWithLongMessage() {
    int count = 100;
    Duration timeout = Duration.ofSeconds(2);

    char[] chars = new char[Configuration.mtuLength() * 15 / 10]; // 1.5 MTU
    Arrays.fill(chars, 'a');
    String msg = new String(chars);

    int request1 = count * 10 / 100; // 10%
    int request2 = request1 * 2; // 20%
    int request3 = request1 * 7; // 70%
    int overall = request1 + request2 + request3; // 100%

    Scheduler scheduler = Schedulers.single();

    createServer(
        connection -> {
          connection
              .outbound()
              .sendString(Flux.range(0, overall).map(i -> i + msg))
              .then()
              .subscribe();
          return connection.onDispose();
        });

    AeronConnection connection1 = createConnection();

    BaseSubscriber<String> subscriber =
        new BaseSubscriber<String>() {
          @Override
          protected void hookOnSubscribe(Subscription subscription) {
            // no-op
          }
        };

    Duration smallInterval = timeout.multipliedBy(30).dividedBy(100); // 30%
    long requestDelay1 = smallInterval.dividedBy(3).toMillis(); // 10%
    long requestDelay2 = requestDelay1 + smallInterval.toMillis(); // 40%
    long requestDelay3 = requestDelay2 + smallInterval.toMillis(); // 70%

    scheduler.schedule(() -> subscriber.request(request1), requestDelay1, TimeUnit.MILLISECONDS);
    scheduler.schedule(() -> subscriber.request(request2), requestDelay2, TimeUnit.MILLISECONDS);
    scheduler.schedule(() -> subscriber.request(request3), requestDelay3, TimeUnit.MILLISECONDS);

    DirectProcessor<String> processor = DirectProcessor.create();
    connection1
        .inbound()
        .receive()
        .asString()
        .take(overall)
        .doOnNext(processor::onNext)
        .subscribe(subscriber);

    StepVerifier.create(processor.buffer(smallInterval).map(List::size))
        .expectNext(request1)
        .expectNext(request2)
        .expectNext(request3)
        .expectNoEvent(smallInterval)
        .thenCancel()
        .verify(timeout);
  }

  private AeronConnection createConnection() {
    return createConnection(null /*handler*/);
  }

  private AeronConnection createConnection(
      Function<? super AeronConnection, ? extends Publisher<Void>> handler) {
    return AeronClient.create(resources)
        .options("localhost", serverPort, serverControlPort)
        .handle(handler)
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

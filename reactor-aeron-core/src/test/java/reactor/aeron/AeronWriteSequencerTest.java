package reactor.aeron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;

class AeronWriteSequencerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(2);

  private static final int PREFETCH = Queues.SMALL_BUFFER_SIZE;
  private static final int FLUX_REQUESTS = PREFETCH * 4;

  private AeronWriteSequencer aeronWriteSequencer;
  private MessagePublication messagePublication;

  @BeforeEach
  void setUp() {
    this.messagePublication = Mockito.mock(MessagePublication.class);
    this.aeronWriteSequencer = new AeronWriteSequencer(messagePublication);
  }

  @Test
  void testWriteMonoRequestInPublicationWhichIsAlreadyDisposed() {
    // the Publication is already disposed
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.empty());
    // long wait task
    Mockito.when(messagePublication.publish(Mockito.any())).thenReturn(Mono.never());

    Mono<ByteBuf> request = Mono.just(toByteBuf("test"));

    StepVerifier.create(aeronWriteSequencer.write(request))
        .expectErrorSatisfies(
            actual -> {
              RuntimeException expected = AeronExceptions.failWithPublicationUnavailable();
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteMonoEmptyInPublicationWhichIsAlreadyDisposed() {
    // the Publication is already disposed
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.empty());

    Mono<ByteBuf> request = Mono.empty();

    StepVerifier.create(aeronWriteSequencer.write(request))
        .expectErrorSatisfies(
            actual -> {
              RuntimeException expected = AeronExceptions.failWithPublicationUnavailable();
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteMonoRequestInPublicationWhichRejectsIncomingRequestsBySomeReasons() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());
    // publication rejects incoming requests
    RuntimeException expected = new RuntimeException("some reasons");
    Mockito.when(messagePublication.publish(Mockito.any())).thenReturn(Mono.error(expected));

    Mono<ByteBuf> request = Mono.just(toByteBuf("test"));

    StepVerifier.create(aeronWriteSequencer.write(request))
        .expectErrorSatisfies(
            actual -> {
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteMonoEmptyInReadyPublication() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    Mono<ByteBuf> request = Mono.empty();

    StepVerifier.create(aeronWriteSequencer.write(request)).expectComplete().verify(TIMEOUT);
  }

  @Test
  void testWriteMonoNeverInReadyPublicationAndThenCancelResultSubscription() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // mono never request
    MonoProcessor<Throwable> latch = MonoProcessor.create();
    Mono<ByteBuf> request =
        Mono.<ByteBuf>never()
            .doOnNext(item -> latch.onError(fail("it was emitted an item")))
            .doOnError(ex -> latch.onError(fail("it was emitted ex: ", ex)))
            .doOnTerminate(() -> latch.onError(fail("it was emitted the completion signal")))
            .doOnCancel(latch::onComplete);

    StepVerifier.create(aeronWriteSequencer.write(request))
        .thenAwait(TIMEOUT.dividedBy(2))
        // cancel the subscription as soon as pause is finished
        .thenCancel()
        .verify(TIMEOUT);

    assertNull(latch.block(TIMEOUT));
  }

  @Test
  void testWriteMonoDelayRequestInReadyPublicationAndThenCancelResultSubscription() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // mono request with delay
    MonoProcessor<Throwable> latch = MonoProcessor.create();
    Mono<ByteBuf> request =
        Mono.delay(TIMEOUT)
            .map(i -> "test" + i)
            .map(this::toByteBuf)
            .doOnNext(item -> latch.onError(fail("it was emitted an item")))
            .doOnError(ex -> latch.onError(fail("it was emitted ex: ", ex)))
            .doOnTerminate(() -> latch.onError(fail("it was emitted the completion signal")))
            .doOnCancel(latch::onComplete);

    StepVerifier.create(aeronWriteSequencer.write(request))
        .thenAwait(TIMEOUT.dividedBy(2))
        // cancel the subscription as soon as pause is finished
        .thenCancel()
        .verify(TIMEOUT);

    assertNull(latch.block(TIMEOUT));
  }

  @Test
  void testWriteMonoRequestInPublicationAndWhileWaitForSendingWillCancelSubscription() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // long wait task
    MonoProcessor<Throwable> latch = MonoProcessor.create();
    Mockito.when(messagePublication.publish(Mockito.any()))
        .thenReturn(
            Mono.<Void>never()
                .log("response")
                .doOnNext(item -> latch.onError(fail("it was emitted an item")))
                .doOnError(ex -> latch.onError(fail("it was emitted ex: ", ex)))
                .doOnTerminate(() -> latch.onError(fail("it was emitted the completion signal")))
                .doOnCancel(latch::onComplete));

    // mono request
    Mono<ByteBuf> request = Mono.just(toByteBuf("test"));

    StepVerifier.create(aeronWriteSequencer.write(request))
        .thenAwait(TIMEOUT.dividedBy(2))
        // cancel the subscription as soon as pause is finished
        .thenCancel()
        .verify(TIMEOUT);

    assertNull(latch.block(TIMEOUT));
  }

  @Test
  void testWriteMonoRequestInPublicationAndWaitForSuccessfulSending() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());
    // long wait task
    Duration delayForFinishEnqueueTask = TIMEOUT.dividedBy(2);
    Mockito.when(messagePublication.publish(Mockito.any()))
        .thenReturn(Mono.delay(delayForFinishEnqueueTask).then());

    // mono request
    Mono<ByteBuf> request = Mono.just(toByteBuf("test"));

    long start = System.nanoTime();
    StepVerifier.create(aeronWriteSequencer.write(request)).expectComplete().verify(TIMEOUT);
    long end = System.nanoTime();

    assertTrue(end - start > delayForFinishEnqueueTask.toNanos());
    assertTrue(end - start < TIMEOUT.toNanos());
  }

  @Test
  void testWriteMonoRequestWithExceptionInPublication() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // mono error request
    RuntimeException expected = new RuntimeException("request error");
    Mono<ByteBuf> request = Mono.error(expected);

    StepVerifier.create(aeronWriteSequencer.write(request))
        .expectErrorSatisfies(
            actual -> {
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteFluxRequestInPublicationAndWhileWaitForAllSendingWillCancelSubscription() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // long wait task
    int fastTasks = PREFETCH;
    int waitingTasks = (PREFETCH - (PREFETCH >> 2)); // see {@link Operators#unboundedOrLimit}
    ReplayProcessor<Integer> latch = ReplayProcessor.create();
    // generates responses
    List<Mono<Void>> responses =
        Stream.concat(
                IntStream.range(0, fastTasks).mapToObj(i -> Mono.<Void>empty()),
                IntStream.range(0, waitingTasks)
                    .mapToObj(
                        i ->
                            Mono.<Void>never()
                                .doOnNext(item -> latch.onError(fail("it was emitted an item")))
                                .doOnError(ex -> latch.onError(fail("it was emitted ex: ", ex)))
                                .doOnTerminate(
                                    () ->
                                        latch.onError(fail("it was emitted the completion signal")))
                                .doOnCancel(() -> latch.onNext(1))))
            .collect(Collectors.toList());

    Mono<Void> first = responses.get(0);
    Mono<Void>[] next = responses.stream().skip(1).toArray(Mono[]::new);

    Mockito.when(messagePublication.publish(Mockito.any())).thenReturn(first, next);

    // flux request
    Flux<ByteBuf> request = Flux.range(0, FLUX_REQUESTS).map(i -> "test" + i).map(this::toByteBuf);

    StepVerifier.create(aeronWriteSequencer.write(request))
        .thenAwait(TIMEOUT)
        // cancel the subscription as soon as pause is finished
        .thenCancel()
        .verify(TIMEOUT);

    assertEquals(Math.min(waitingTasks, PREFETCH), latch.take(TIMEOUT).count().block().intValue());
  }

  @Test
  void testWriteFluxRequestInPublicationAndWaitForSuccessfulAllSending() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());
    // long wait task
    Duration delayForFinishEnqueueTasks = TIMEOUT.dividedBy(2).dividedBy(FLUX_REQUESTS);
    Mockito.when(messagePublication.publish(Mockito.any()))
        .thenReturn(Mono.delay(delayForFinishEnqueueTasks).then());

    // flux request
    Flux<ByteBuf> request = Flux.range(0, FLUX_REQUESTS).map(i -> "test" + i).map(this::toByteBuf);

    long start = System.nanoTime();
    StepVerifier.create(aeronWriteSequencer.write(request)).expectComplete().verify(TIMEOUT);
    long end = System.nanoTime();

    assertTrue(
        end - start > delayForFinishEnqueueTasks.multipliedBy(FLUX_REQUESTS / PREFETCH).toNanos());
    assertTrue(end - start < TIMEOUT.toNanos());
  }

  @Test
  void testWriteFluxDelayFirstRequestInReadyPublicationAndThenCancelSubscription() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // flux request with delay
    MonoProcessor<Throwable> latch = MonoProcessor.create();
    Flux<ByteBuf> request =
        Flux.range(0, FLUX_REQUESTS)
            .delayElements(TIMEOUT)
            .map(i -> "test" + i)
            .map(this::toByteBuf)
            .doOnNext(item -> latch.onError(fail("it was emitted an item")))
            .doOnError(ex -> latch.onError(fail("it was emitted ex: ", ex)))
            .doOnTerminate(() -> latch.onError(fail("it was emitted the completion signal")))
            .doOnCancel(latch::onComplete);

    StepVerifier.create(aeronWriteSequencer.write(request))
        .thenAwait(TIMEOUT.dividedBy(2))
        // cancel the subscription as soon as pause is finished
        .thenCancel()
        .verify(TIMEOUT);

    assertNull(latch.block(TIMEOUT));
  }

  @Test
  void testWriteFluxRequestWithExceptionInPublicationAndWhileWaitForAllPreviousSendingTasks() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    // long wait tasks
    int waitingTasks = 10;
    waitingTasks = Math.min(waitingTasks, PREFETCH - 1);
    RuntimeException expected = new RuntimeException("request error");
    ReplayProcessor<Integer> latch = ReplayProcessor.create();

    // generates responses
    Mockito.when(messagePublication.publish(Mockito.any()))
        .thenReturn(
            Mono.<Void>never()
                .doOnNext(item -> latch.onError(fail("it was emitted an item")))
                .doOnError(ex -> latch.onError(fail("it was emitted ex: ", ex)))
                .doOnTerminate(() -> latch.onError(fail("it was emitted the completion signal")))
                .doOnCancel(() -> latch.onNext(1)));

    // flux request
    Flux<ByteBuf> request =
        Flux.range(0, waitingTasks)
            .map(i -> "test" + i)
            .map(this::toByteBuf)
            .concatWith(Mono.error(expected));

    StepVerifier.create(aeronWriteSequencer.write(request))
        .expectErrorSatisfies(
            actual -> {
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);

    assertEquals(waitingTasks, latch.take(TIMEOUT).count().block().intValue());
  }

  @Test
  void testWriteFluxRequestInPublicationWhichIsAlreadyDisposed() {
    // the Publication is already disposed
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.empty());
    Mockito.when(messagePublication.isDisposed()).thenReturn(false);
    // long wait task
    Mockito.when(messagePublication.publish(Mockito.any())).thenReturn(Mono.never());
    RuntimeException expected = AeronExceptions.failWithPublicationUnavailable();

    // flux request
    Flux<ByteBuf> request =
        Flux.range(0, FLUX_REQUESTS)
            .subscribeOn(Schedulers.single())
            .publishOn(Schedulers.single())
            .map(i -> "test" + i)
            .map(this::toByteBuf);

    StepVerifier.create(aeronWriteSequencer.write(request).subscribeOn(Schedulers.single()))
        .expectErrorSatisfies(
            actual -> {
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteFluxEmptyInPublicationWhichIsAlreadyDisposed() {
    // the Publication is already disposed
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.empty());

    RuntimeException expected = AeronExceptions.failWithPublicationUnavailable();

    // flux empty request
    Flux<ByteBuf> request = Flux.empty();

    StepVerifier.create(aeronWriteSequencer.write(request))
        .expectErrorSatisfies(
            actual -> {
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteFluxRequestInPublicationWhichRejectsIncomingRequestsBySomeReasons() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());
    // publication rejects incoming requests
    RuntimeException expected = new RuntimeException("some reasons");

    Mono<Void> first = Mono.empty();
    Mono<Void>[] next = new Mono[] {Mono.<Void>error(expected)};
    Mockito.when(messagePublication.publish(Mockito.any())).thenReturn(first, next);

    // flux request
    Flux<ByteBuf> request =
        Flux.range(0, FLUX_REQUESTS)
            .subscribeOn(Schedulers.parallel())
            .publishOn(Schedulers.parallel())
            .map(i -> "test" + i)
            .map(this::toByteBuf);

    StepVerifier.create(aeronWriteSequencer.write(request).subscribeOn(Schedulers.parallel()))
        .expectErrorSatisfies(
            actual -> {
              assertEquals(expected.getClass(), actual.getClass());
              assertEquals(expected.getMessage(), actual.getMessage());
            })
        .verify(TIMEOUT);
  }

  @Test
  void testWriteFluxEmptyInReadyPublication() {
    // the Publication is ready
    Mockito.when(messagePublication.onDispose()).thenReturn(Mono.never());

    Flux<ByteBuf> request = Flux.empty();

    StepVerifier.create(aeronWriteSequencer.write(request)).expectComplete().verify(TIMEOUT);
  }

  private ByteBuf toByteBuf(String string) {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    buffer.writeCharSequence(string, Charset.defaultCharset());
    return buffer;
  }
}

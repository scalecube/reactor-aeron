package reactor.aeron.client;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.aeron.AeronEventLoop;
import reactor.aeron.AeronOptions;
import reactor.aeron.AeronResources;
import reactor.aeron.Connection;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class AeronClient {

  private static final AtomicInteger STREAM_ID_COUNTER = new AtomicInteger();

  private final AeronClientSettings settings;

  private AeronClient(AeronClientSettings settings) {
    this.settings = settings;
  }

  /**
   * Create aeron client.
   *
   * @param aeronResources aeronResources
   * @return aeron client
   */
  public static AeronClient create(AeronResources aeronResources) {
    return create("client", aeronResources);
  }

  /**
   * Create aeron client.
   *
   * @param name name
   * @param aeronResources aeronResources
   * @return aeron client
   */
  public static AeronClient create(String name, AeronResources aeronResources) {
    return new AeronClient(
        AeronClientSettings.builder().name(name).aeronResources(aeronResources).build());
  }

  public Mono<? extends Connection> connect() {
    return connect(settings.options());
  }

  public Mono<? extends Connection> connect(AeronOptions options) {
    return Mono.defer(() -> connect0(options));
  }

  private Mono<? extends Connection> connect0(AeronOptions options) {
    return Mono.defer(
        () -> {
          AeronClientSettings settings = this.settings.options(options);

          int clientControlStreamId = STREAM_ID_COUNTER.incrementAndGet();
          Supplier<Integer> clientSessionStreamIdCounter = STREAM_ID_COUNTER::incrementAndGet;

          AeronClientConnector clientConnector =
              new AeronClientConnector(
                  settings, clientControlStreamId, clientSessionStreamIdCounter);

          String category = Optional.ofNullable(settings.name()).orElse("client");
          AeronResources resources = settings.aeronResources();
          String clientChannel = settings.options().clientChannel();
          AeronEventLoop eventLoop = resources.nextEventLoop();

          AtomicReference<Disposable> disposableUnavailable = new AtomicReference<>();

          return resources
              .controlSubscription(
                  category,
                  clientChannel,
                  clientControlStreamId,
                  clientConnector,
                  eventLoop,
                  image -> {
                    Disposable disposable = disposableUnavailable.get();
                    if (disposable != null) {
                      disposable.dispose();
                    }
                  },
                  image -> disposableUnavailable.set(Schedulers.single().schedule(clientConnector::dispose, 5, TimeUnit.SECONDS)))
              .flatMap(
                  controlSubscription -> {
                    clientConnector.onSubscription(controlSubscription);
                    return clientConnector
                        .start()
                        .doOnError(
                            ex -> {
                              controlSubscription.dispose();
                              clientConnector.dispose();
                            })
                        .doOnSuccess(
                            connection -> {
                              settings
                                  .handler() //
                                  .apply(connection)
                                  .subscribe(connection.disposeSubscriber());
                              connection
                                  .onDispose()
                                  .doFinally(
                                      s -> {
                                        controlSubscription.dispose();
                                        clientConnector.dispose();
                                      })
                                  .subscribe(
                                      null,
                                      th -> {
                                        // no-op
                                      });
                            });
                  });
        });
  }

  /**
   * Apply {@link AeronOptions} on the given options consumer.
   *
   * @param options a consumer aeron client options
   * @return a new {@link AeronClient}
   */
  public AeronClient options(Consumer<AeronOptions.Builder> options) {
    return new AeronClient(settings.options(options));
  }

  /**
   * Attach an IO handler to react on connected client.
   *
   * @param handler an IO handler that can dispose underlying connection when {@link Publisher}
   *     terminates.
   * @return a new {@link AeronClient}
   */
  public AeronClient handle(Function<? super Connection, ? extends Publisher<Void>> handler) {
    return new AeronClient(settings.handler(handler));
  }
}

package reactor.aeron.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronEventLoop;
import reactor.aeron.AeronEventLoopGroup;
import reactor.aeron.AeronInbound;
import reactor.aeron.AeronOutbound;
import reactor.aeron.FragmentMapper;
import reactor.aeron.ImageAgent;
import reactor.aeron.OnDisposable;
import reactor.aeron.PublicationAgent;
import reactor.aeron.SubscriptionAgent;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class AeronResources implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  private static final Supplier<IdleStrategy> defaultBackoffIdleStrategySupplier =
      () -> new BackoffIdleStrategy(0, 0, 0, 1);

  // Settings

  private int pollFragmentLimit = 32;
  private int numOfWorkers = Runtime.getRuntime().availableProcessors();

  private Aeron.Context aeronContext =
      new Aeron.Context().errorHandler(th -> logger.warn("Aeron exception occurred: " + th, th));

  private MediaDriver.Context mediaContext =
      new MediaDriver.Context()
          .errorHandler(th -> logger.warn("Exception occurred on MediaDriver: " + th, th))
          .warnIfDirectoryExists(true)
          .dirDeleteOnStart(true)
          .dirDeleteOnShutdown(true)
          // low latency settings
          .termBufferSparseFile(false)
          // explicit range of reserved session ids
          .publicationReservedSessionIdLow(0)
          .publicationReservedSessionIdHigh(Integer.MAX_VALUE);

  private Supplier<IdleStrategy> workerIdleStrategySupplier = defaultBackoffIdleStrategySupplier;

  // State
  private Aeron aeron;
  private MediaDriver mediaDriver;
  private AeronEventLoopGroup eventLoopGroup;

  private Scheduler scheduler = Schedulers.newSingle("AeronResources", true);

  // Lifecycle
  private final MonoProcessor<Void> start = MonoProcessor.create();
  private final MonoProcessor<Void> onStart = MonoProcessor.create();
  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  /**
   * Default constructor. Setting up start and dispose routings. See methods: {@link #doStart()} and
   * {@link #doDispose()}.
   */
  public AeronResources() {
    start
        .then(doStart())
        .doOnSuccess(avoid -> onStart.onComplete())
        .doOnError(onStart::onError)
        .subscribe(
            null,
            th -> {
              logger.error("{} failed to start, cause: {}", this, th.toString());
              dispose();
            });

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            null,
            th -> logger.warn("{} failed on doDispose(): {}", this, th.toString()),
            () -> logger.debug("Disposed {}", this));
  }

  /**
   * Copy constructor.
   *
   * @param ac aeron context
   * @param mdc media driver context
   */
  private AeronResources(AeronResources that, Aeron.Context ac, MediaDriver.Context mdc) {
    this();
    this.pollFragmentLimit = that.pollFragmentLimit;
    this.numOfWorkers = that.numOfWorkers;
    this.workerIdleStrategySupplier = that.workerIdleStrategySupplier;
    copy(ac);
    copy(mdc);
  }

  private AeronResources copy() {
    return new AeronResources(this, aeronContext, mediaContext);
  }

  private void copy(MediaDriver.Context mdc) {
    mediaContext
        .aeronDirectoryName(mdc.aeronDirectoryName())
        .dirDeleteOnStart(mdc.dirDeleteOnStart())
        .imageLivenessTimeoutNs(mdc.imageLivenessTimeoutNs())
        .mtuLength(mdc.mtuLength())
        .driverTimeoutMs(mdc.driverTimeoutMs())
        .errorHandler(mdc.errorHandler())
        .threadingMode(mdc.threadingMode())
        .applicationSpecificFeedback(mdc.applicationSpecificFeedback())
        .cachedEpochClock(mdc.cachedEpochClock())
        .cachedNanoClock(mdc.cachedNanoClock())
        .clientLivenessTimeoutNs(mdc.clientLivenessTimeoutNs())
        .conductorIdleStrategy(mdc.conductorIdleStrategy())
        .conductorThreadFactory(mdc.conductorThreadFactory())
        .congestControlSupplier(mdc.congestionControlSupplier())
        .counterFreeToReuseTimeoutNs(mdc.counterFreeToReuseTimeoutNs())
        .countersManager(mdc.countersManager())
        .countersMetaDataBuffer(mdc.countersMetaDataBuffer())
        .countersValuesBuffer(mdc.countersValuesBuffer())
        .epochClock(mdc.epochClock())
        .warnIfDirectoryExists(mdc.warnIfDirectoryExists())
        .useWindowsHighResTimer(mdc.useWindowsHighResTimer())
        .useConcurrentCountersManager(mdc.useConcurrentCountersManager())
        .unicastFlowControlSupplier(mdc.unicastFlowControlSupplier())
        .multicastFlowControlSupplier(mdc.multicastFlowControlSupplier())
        .timerIntervalNs(mdc.timerIntervalNs())
        .termBufferSparseFile(mdc.termBufferSparseFile())
        .tempBuffer(mdc.tempBuffer())
        .systemCounters(mdc.systemCounters())
        .statusMessageTimeoutNs(mdc.statusMessageTimeoutNs())
        .spiesSimulateConnection(mdc.spiesSimulateConnection())
        .sharedThreadFactory(mdc.sharedThreadFactory())
        .sharedNetworkThreadFactory(mdc.sharedNetworkThreadFactory())
        .sharedNetworkIdleStrategy(mdc.sharedNetworkIdleStrategy())
        .sharedIdleStrategy(mdc.sharedIdleStrategy())
        .senderThreadFactory(mdc.senderThreadFactory())
        .senderIdleStrategy(mdc.senderIdleStrategy())
        .sendChannelEndpointSupplier(mdc.sendChannelEndpointSupplier())
        .receiverThreadFactory(mdc.receiverThreadFactory())
        .receiverIdleStrategy(mdc.receiverIdleStrategy())
        .receiveChannelEndpointThreadLocals(mdc.receiveChannelEndpointThreadLocals())
        .receiveChannelEndpointSupplier(mdc.receiveChannelEndpointSupplier())
        .publicationUnblockTimeoutNs(mdc.publicationUnblockTimeoutNs())
        .publicationTermBufferLength(mdc.publicationTermBufferLength())
        .publicationReservedSessionIdLow(mdc.publicationReservedSessionIdLow())
        .publicationReservedSessionIdHigh(mdc.publicationReservedSessionIdHigh())
        .publicationLingerTimeoutNs(mdc.publicationLingerTimeoutNs())
        .publicationConnectionTimeoutNs(mdc.publicationConnectionTimeoutNs())
        .performStorageChecks(mdc.performStorageChecks())
        .nanoClock(mdc.nanoClock())
        .lossReport(mdc.lossReport())
        .ipcTermBufferLength(mdc.ipcTermBufferLength())
        .ipcMtuLength(mdc.ipcMtuLength())
        .initialWindowLength(mdc.initialWindowLength())
        .filePageSize(mdc.filePageSize())
        .errorLog(mdc.errorLog());
  }

  private void copy(Aeron.Context ac) {
    aeronContext
        .resourceLingerDurationNs(ac.resourceLingerDurationNs())
        .keepAliveIntervalNs(ac.keepAliveIntervalNs())
        .errorHandler(ac.errorHandler())
        .driverTimeoutMs(ac.driverTimeoutMs())
        .availableImageHandler(ac.availableImageHandler())
        .unavailableImageHandler(ac.unavailableImageHandler())
        .idleStrategy(ac.idleStrategy())
        .aeronDirectoryName(ac.aeronDirectoryName())
        .availableCounterHandler(ac.availableCounterHandler())
        .unavailableCounterHandler(ac.unavailableCounterHandler())
        .useConductorAgentInvoker(ac.useConductorAgentInvoker())
        .threadFactory(ac.threadFactory())
        .epochClock(ac.epochClock())
        .clientLock(ac.clientLock())
        .nanoClock(ac.nanoClock());
  }

  private static String generateRandomTmpDirName() {
    return IoUtil.tmpDirName()
        + "aeron"
        + '-'
        + System.getProperty("user.name", "default")
        + '-'
        + UUID.randomUUID().toString();
  }

  /**
   * Applies modifier and produces new {@code AeronResources} object.
   *
   * @param o modifier operator
   * @return new {@code AeronResources} object
   */
  public AeronResources aeron(UnaryOperator<Aeron.Context> o) {
    AeronResources c = copy();
    Aeron.Context ac = o.apply(c.aeronContext);
    return new AeronResources(this, ac, c.mediaContext);
  }

  /**
   * Applies modifier and produces new {@code AeronResources} object.
   *
   * @param o modifier operator
   * @return new {@code AeronResources} object
   */
  public AeronResources media(UnaryOperator<MediaDriver.Context> o) {
    AeronResources c = copy();
    MediaDriver.Context mdc = o.apply(c.mediaContext);
    return new AeronResources(this, c.aeronContext, mdc);
  }

  /**
   * Set to use temp directory instead of default aeron directory.
   *
   * @return new {@code AeronResources} object
   */
  public AeronResources useTmpDir() {
    return media(mdc -> mdc.aeronDirectoryName(generateRandomTmpDirName()));
  }

  /**
   * Shortcut for {@code numOfWorkers(1)}.
   *
   * @return new {@code AeronResources} object
   */
  public AeronResources singleWorker() {
    return numOfWorkers(1);
  }

  /**
   * Setting number of worker threads.
   *
   * @param n number of worker threads
   * @return new {@code AeronResources} object
   */
  public AeronResources numOfWorkers(int n) {
    AeronResources c = copy();
    c.numOfWorkers = n;
    return c;
  }

  /**
   * Settings fragment limit for polling.
   *
   * @param pollFragmentLimit fragment limit for polling
   * @return new {@code AeronResources} object
   */
  public AeronResources pollFragmentLimit(int pollFragmentLimit) {
    AeronResources c = copy();
    c.pollFragmentLimit = pollFragmentLimit;
    return c;
  }

  /**
   * Setter for supplier of {@code IdleStrategy} for worker thread(s).
   *
   * @param s supplier of {@code IdleStrategy} for worker thread(s)
   * @return @return new {@code AeronResources} object
   */
  public AeronResources workerIdleStrategySupplier(Supplier<IdleStrategy> s) {
    AeronResources c = copy();
    c.workerIdleStrategySupplier = s;
    return c;
  }

  /**
   * Starting up this resources instance if not started already.
   *
   * @return started {@code AeronResources} object
   */
  public Mono<AeronResources> start() {
    return Mono.defer(
        () -> {
          start.onComplete();
          return onStart.thenReturn(this);
        });
  }

  private Mono<Void> doStart() {
    return Mono.fromRunnable(
        () -> {
          mediaDriver = MediaDriver.launchEmbedded(mediaContext);

          aeronContext.aeronDirectoryName(mediaDriver.aeronDirectoryName());

          aeron = Aeron.connect(aeronContext);

          eventLoopGroup =
              new AeronEventLoopGroup("reactor-aeron", numOfWorkers, workerIdleStrategySupplier);

          logger.debug(
              "{} has initialized embedded media driver, aeron directory: {}",
              this,
              aeronContext.aeronDirectoryName());
        });
  }

  /**
   * Shortcut method for {@code eventLoopGroup.next()}.
   *
   * @return {@code AeronEventLoop} instance
   */
  public AeronEventLoop nextEventLoop() {
    return eventLoopGroup.next();
  }

  /**
   * Returns already used first event loop. See {@code eventLoopGroup.first()}.
   *
   * @return {@code AeronEventLoop} instance
   */
  public AeronEventLoop firstEventLoop() {
    return eventLoopGroup.first();
  }

  /**
   * Returns subscription inbound which registered in event loop.
   *
   * @param channel subscription channel
   * @param streamId subscription stream id
   * @param mapper mapper
   * @return mono result
   */
  public <T> Mono<AeronInbound<T>> inbound(String channel, int streamId, FragmentMapper<T> mapper) {
    return subscription(channel, streamId, null, null)
        .map(
            subscription -> {
              AeronEventLoop eventLoop = nextEventLoop();
              SubscriptionAgent<T> agent = new SubscriptionAgent<>(subscription, mapper, true);
              eventLoop.register(agent);
              return agent;
            });
  }

  /**
   * Returns image inbound which registered in event loop.
   *
   * @param channel image channel
   * @param streamId image stream id
   * @param mapper mapper
   * @return mono result
   */
  public <T> Mono<AeronInbound<T>> imageInbound(
      String channel, int streamId, FragmentMapper<T> mapper) {
    return Mono.defer(
        () -> {
          if (!ChannelUri.parse(channel).containsKey(CommonContext.SESSION_ID_PARAM_NAME)) {
            throw new IllegalArgumentException("channel should be unique for image inbound");
          }
          MonoProcessor<Image> imageCallback = MonoProcessor.create();
          return subscription(channel, streamId, imageCallback::onNext, null)
              .flatMap(subscription -> imageCallback)
              .map(
                  image -> {
                    AeronEventLoop eventLoop = nextEventLoop();
                    ImageAgent<T> agent = new ImageAgent<>(image, mapper, true);
                    eventLoop.register(agent);
                    return agent;
                  });
        });
  }

  /**
   * Returns outbound which registered in event loop.
   *
   * @param channel target channel
   * @param streamId target stream id
   * @return mono result
   */
  public Mono<AeronOutbound> outbound(String channel, int streamId) {
    return publication(channel, streamId)
        .map(
            publication -> {
              AeronEventLoop eventLoop = nextEventLoop();
              PublicationAgent agent = new PublicationAgent(publication);
              eventLoop.register(agent);
              return agent;
            });
  }

  /**
   * Creates aeron {@link ExclusivePublication}.
   *
   * @param channel aeron channel
   * @param streamId aeron stream id
   * @return mono result
   */
  Mono<Publication> publication(String channel, int streamId) {
    return Mono.defer(
        () ->
            aeronPublication(channel, streamId)
                .subscribeOn(scheduler)
                .doOnError(
                    ex ->
                        logger.error(
                            "{} failed on aeronPublication(), channel: {}, cause: {}",
                            this,
                            channel,
                            ex.toString())));
  }

  private Mono<Publication> aeronPublication(String channel, int streamId) {
    return Mono.fromCallable(
        () -> {
          logger.debug("Adding aeron.Publication for channel {}", channel);
          long startTime = System.nanoTime();

          Publication publication = aeron.addExclusivePublication(channel, streamId);

          long endTime = System.nanoTime();
          long spent = Duration.ofNanos(endTime - startTime).toNanos();
          logger.debug("Added aeron.Publication for channel {}, spent: {} ns", channel, spent);

          return publication;
        });
  }

  /**
   * Creates aeron {@link Subscription}.
   *
   * @param channel aeron channel
   * @param streamId aeron stream id
   * @param onImageAvailable available image handler; optional
   * @param onImageUnavailable unavailable image handler; optional
   * @return mono result
   */
  Mono<Subscription> subscription(
      String channel,
      int streamId,
      Consumer<Image> onImageAvailable,
      Consumer<Image> onImageUnavailable) {
    return Mono.defer(
        () ->
            aeronSubscription(channel, streamId, onImageAvailable, onImageUnavailable)
                .subscribeOn(scheduler)
                .doOnError(
                    ex ->
                        logger.error(
                            "{} failed on aeronSubscription(), channel: {}, cause: {}",
                            this,
                            channel,
                            ex.toString())));
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  private Mono<Subscription> aeronSubscription(
      String channel,
      int streamId,
      Consumer<Image> onImageAvailable,
      Consumer<Image> onImageUnavailable) {

    return Mono.fromCallable(
        () -> {
          logger.debug("Adding aeron.Subscription for channel {}", channel);
          long startTime = System.nanoTime();

          Subscription subscription =
              aeron.addSubscription(
                  channel,
                  streamId,
                  image -> {
                    logger.debug(
                        "{} onImageAvailable: {} {}",
                        this,
                        Integer.toHexString(image.sessionId()),
                        image.sourceIdentity());
                    Optional.ofNullable(onImageAvailable).ifPresent(c -> c.accept(image));
                  },
                  image -> {
                    logger.debug(
                        "{} onImageUnavailable: {} {}",
                        this,
                        Integer.toHexString(image.sessionId()),
                        image.sourceIdentity());
                    Optional.ofNullable(onImageUnavailable).ifPresent(c -> c.accept(image));
                  });

          long endTime = System.nanoTime();
          long spent = Duration.ofNanos(endTime - startTime).toNanos();
          logger.debug("Added aeron.Subscription for channel {}, spent: {} ns", channel, spent);

          return subscription;
        });
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  private Mono<Void> doDispose() {
    return Mono.fromRunnable(
        () -> {
          CloseHelper.quietClose(eventLoopGroup);
          CloseHelper.quietClose(aeron);
          CloseHelper.quietClose(mediaDriver);
          scheduler.dispose();
          logger.debug("Disposed {}", this);
        });
  }

  @Override
  public String toString() {
    return "AeronResources" + Integer.toHexString(System.identityHashCode(this));
  }
}

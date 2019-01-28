package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import java.io.File;
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
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public final class AeronResources implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  // Settings

  private int pollFragmentLimit = 8192;
  private int publicationPendingLimit = 8192;
  private int requestPendingCount = Queues.SMALL_BUFFER_SIZE;
  private int numOfWorkers = Runtime.getRuntime().availableProcessors();

  private Aeron.Context aeronContext =
      new Aeron.Context().errorHandler(th -> logger.warn("Aeron exception occurred: " + th, th));

  private MediaDriver.Context mediaContext =
      new MediaDriver.Context()
          .errorHandler(th -> logger.warn("Exception occurred on MediaDriver: " + th, th))
          .warnIfDirectoryExists(true)
          .dirDeleteOnStart(true)
          // explicit range of reserved session ids
          .publicationReservedSessionIdLow(0)
          .publicationReservedSessionIdHigh(Integer.MAX_VALUE);

  private Supplier<IdleStrategy> workerIdleStrategySupplier =
      () -> new BackoffIdleStrategy(1, 1, 1, 100);

  // State
  private Aeron aeron;
  private MediaDriver mediaDriver;
  private AeronEventLoopGroup eventLoopGroup;

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
    this.publicationPendingLimit = that.publicationPendingLimit;
    this.requestPendingCount = that.requestPendingCount;
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
        .keepAliveInterval(ac.keepAliveInterval())
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
   * Settings pending task limit per publication.
   *
   * @param publicationPendingLimit pending task limit per publication
   * @return new {@code AeronResources} object
   */
  public AeronResources publicationPendingLimit(int publicationPendingLimit) {
    AeronResources c = copy();
    c.publicationPendingLimit = publicationPendingLimit;
    return c;
  }

  int publicationPendingLimit() {
    return publicationPendingLimit;
  }

  /**
   * Settings pending task count per stream request.
   *
   * @param requestPendingCount pending task count per request
   * @return new {@code AeronResources} object
   */
  public AeronResources requestPendingCount(int requestPendingCount) {
    AeronResources c = copy();
    c.requestPendingCount = requestPendingCount;
    return c;
  }

  int requestPendingCount() {
    return requestPendingCount;
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

          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(() -> deleteAeronDirectory(aeronContext.aeronDirectory())));

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
  AeronEventLoop nextEventLoop() {
    return eventLoopGroup.next();
  }

  /**
   * Returns already used first event loop. See {@code eventLoopGroup.first()}.
   *
   * @return {@code AeronEventLoop} instance
   */
  AeronEventLoop firstEventLoop() {
    return eventLoopGroup.first();
  }

  /**
   * Creates and registers {@link DefaultAeronInbound}.
   *
   * @param image aeron image
   * @param subscription subscription
   * @param eventLoop aeron event lopop
   * @return mono result
   */
  Mono<DefaultAeronInbound> inbound(
      Image image, MessageSubscription subscription, AeronEventLoop eventLoop) {
    return Mono.defer(
        () -> {
          DefaultAeronInbound inbound =
              new DefaultAeronInbound(image, eventLoop, subscription, pollFragmentLimit);
          return eventLoop
              .registerInbound(inbound)
              .doOnError(
                  ex ->
                      logger.error(
                          "{} failed on registerInbound(), cause: {}", this, ex.toString()));
        });
  }

  /**
   * Creates aeron {@link ExclusivePublication} then wraps it into {@link MessagePublication}.
   * Result message publication will be assigned to event loop.
   *
   * @param channel aeron channel
   * @param streamId aeron stream id
   * @param options aeorn options
   * @param eventLoop aeron event loop
   * @return mono result
   */
  Mono<MessagePublication> publication(
      String channel, int streamId, AeronOptions options, AeronEventLoop eventLoop) {
    return Mono.defer(
        () ->
            aeronPublication(channel, streamId)
                .subscribeOn(Schedulers.parallel())
                .doOnError(
                    ex ->
                        logger.error(
                            "{} failed on aeronPublication(), channel: {}, cause: {}",
                            this,
                            channel,
                            ex.toString()))
                .flatMap(
                    aeronPublication ->
                        eventLoop
                            .registerPublication(
                                new MessagePublication(aeronPublication, options, eventLoop))
                            .doOnError(
                                ex -> {
                                  logger.error(
                                      "{} failed on registerPublication(), cause: {}",
                                      this,
                                      ex.toString());
                                  if (!aeronPublication.isClosed()) {
                                    aeronPublication.close();
                                  }
                                })));
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

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  /**
   * Creates aeron {@link Subscription} then wraps it into {@link MessageSubscription}. Result
   * message subscription will be assigned to event loop.
   *
   * @param channel aeron channel
   * @param streamId aeron stream id
   * @param eventLoop aeron event loop
   * @param onImageAvailable available image handler; optional
   * @param onImageUnavailable unavailable image handler; optional
   * @return mono result
   */
  Mono<MessageSubscription> subscription(
      String channel,
      int streamId,
      AeronEventLoop eventLoop,
      Consumer<Image> onImageAvailable,
      Consumer<Image> onImageUnavailable) {

    return Mono.defer(
        () ->
            aeronSubscription(channel, streamId, onImageAvailable, onImageUnavailable)
                .subscribeOn(Schedulers.parallel())
                .doOnError(
                    ex ->
                        logger.error(
                            "{} failed on aeronSubscription(), channel: {}, cause: {}",
                            this,
                            channel,
                            ex.toString()))
                .flatMap(
                    aeronSubscription ->
                        eventLoop
                            .registerSubscription(
                                new MessageSubscription(aeronSubscription, eventLoop))
                            .doOnError(
                                ex -> {
                                  logger.error(
                                      "{} failed on registerSubscription(), cause: {}",
                                      this,
                                      ex.toString());
                                  if (!aeronSubscription.isClosed()) {
                                    aeronSubscription.close();
                                  }
                                })));
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
    return Mono.defer(
        () -> {
          logger.debug("Disposing {}", this);

          return Mono //
              .fromRunnable(eventLoopGroup::dispose)
              .then(eventLoopGroup.onDispose())
              .doFinally(
                  s -> {
                    CloseHelper.quietClose(aeron);

                    CloseHelper.quietClose(mediaDriver);

                    Optional.ofNullable(aeronContext)
                        .ifPresent(c -> IoUtil.delete(c.aeronDirectory(), true));
                  });
        });
  }

  private void deleteAeronDirectory(File aeronDirectory) {
    if (aeronDirectory.exists()) {
      IoUtil.delete(aeronDirectory, true);
      logger.debug("{} deleted aeron directory {}", this, aeronDirectory);
    }
  }

  @Override
  public String toString() {
    return "AeronResources" + Integer.toHexString(System.identityHashCode(this));
  }
}

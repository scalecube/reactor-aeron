package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import java.io.File;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
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

public final class AeronArchiveResources implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronArchiveResources.class);

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
          // low latency settings
          .termBufferSparseFile(false)
          // explicit range of reserved session ids
          .publicationReservedSessionIdLow(0)
          .publicationReservedSessionIdHigh(Integer.MAX_VALUE);

  private Archive.Context archiveContext =
      new Archive.Context()
          .errorHandler(th -> logger.warn("Exception occurred on Archive: " + th, th));

  private Supplier<IdleStrategy> workerIdleStrategySupplier = defaultBackoffIdleStrategySupplier;

  // State
  private Aeron aeron;
  private ArchivingMediaDriver archivingMediaDriver;
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
  public AeronArchiveResources() {
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
  private AeronArchiveResources(
      AeronArchiveResources that, Aeron.Context ac, MediaDriver.Context mdc) {
    this();
    this.pollFragmentLimit = that.pollFragmentLimit;
    this.numOfWorkers = that.numOfWorkers;
    this.workerIdleStrategySupplier = that.workerIdleStrategySupplier;
    copy(ac);
    copy(mdc);
  }

  private AeronArchiveResources copy() {
    return new AeronArchiveResources(this, aeronContext, mediaContext);
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
   * Applies modifier and produces new {@code AeronArchiveResources} object.
   *
   * @param o modifier operator
   * @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources aeron(UnaryOperator<Aeron.Context> o) {
    AeronArchiveResources c = copy();
    Aeron.Context ac = o.apply(c.aeronContext);
    return new AeronArchiveResources(this, ac, c.mediaContext);
  }

  /**
   * Applies modifier and produces new {@code AeronArchiveResources} object.
   *
   * @param o modifier operator
   * @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources media(UnaryOperator<MediaDriver.Context> o) {
    AeronArchiveResources c = copy();
    MediaDriver.Context mdc = o.apply(c.mediaContext);
    return new AeronArchiveResources(this, c.aeronContext, mdc);
  }

  /**
   * Set to use temp directory instead of default aeron directory.
   *
   * @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources useTmpDir() {
    return media(mdc -> mdc.aeronDirectoryName(generateRandomTmpDirName()));
  }

  /**
   * Shortcut for {@code numOfWorkers(1)}.
   *
   * @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources singleWorker() {
    return numOfWorkers(1);
  }

  /**
   * Setting number of worker threads.
   *
   * @param n number of worker threads
   * @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources numOfWorkers(int n) {
    AeronArchiveResources c = copy();
    c.numOfWorkers = n;
    return c;
  }

  /**
   * Settings fragment limit for polling.
   *
   * @param pollFragmentLimit fragment limit for polling
   * @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources pollFragmentLimit(int pollFragmentLimit) {
    AeronArchiveResources c = copy();
    c.pollFragmentLimit = pollFragmentLimit;
    return c;
  }

  /**
   * Setter for supplier of {@code IdleStrategy} for worker thread(s).
   *
   * @param s supplier of {@code IdleStrategy} for worker thread(s)
   * @return @return new {@code AeronArchiveResources} object
   */
  public AeronArchiveResources workerIdleStrategySupplier(Supplier<IdleStrategy> s) {
    AeronArchiveResources c = copy();
    c.workerIdleStrategySupplier = s;
    return c;
  }

  /**
   * Starting up this resources instance if not started already.
   *
   * @return started {@code AeronArchiveResources} object
   */
  public Mono<AeronArchiveResources> start() {
    return Mono.defer(
        () -> {
          start.onComplete();
          return onStart.thenReturn(this);
        });
  }

  private Mono<Void> doStart() {
    return Mono.fromRunnable(
        () -> {
          archivingMediaDriver = ArchivingMediaDriver.launch(mediaContext, archiveContext);

          aeronContext.aeronDirectoryName(archivingMediaDriver.mediaDriver().aeronDirectoryName());

          aeron = Aeron.connect(aeronContext);

          eventLoopGroup =
              new AeronEventLoopGroup(
                  "reactor-aeron-archive", numOfWorkers, workerIdleStrategySupplier);

          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(
                      () ->
                          deleteAeronDirectory(
                              archivingMediaDriver.mediaDriver().aeronDirectoryName())));

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

  @Override
  public void dispose() {
    dispose.onComplete();
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

                    CloseHelper.quietClose(archivingMediaDriver);

                    Optional.ofNullable(aeronContext)
                        .ifPresent(c -> IoUtil.delete(c.aeronDirectory(), true));
                  });
        });
  }

  private void deleteAeronDirectory(String aeronDirectoryName) {
    File aeronDirectory = Paths.get(aeronDirectoryName).toFile();
    if (aeronDirectory.exists()) {
      IoUtil.delete(aeronDirectory, true);
      logger.debug("{} deleted aeron directory {}", this, aeronDirectoryName);
    }
  }

  @Override
  public String toString() {
    return "AeronArchiveResources" + Integer.toHexString(System.identityHashCode(this));
  }
}

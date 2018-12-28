package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import java.io.File;
import java.util.Optional;
import java.util.function.Consumer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class AeronResources implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronResources.class);

  private final AeronResourcesConfig config;

  private final MonoProcessor<Void> start = MonoProcessor.create();
  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private Aeron aeron;
  private MediaDriver mediaDriver;
  private AeronEventLoopGroup eventLoopGroup;

  private AeronResources(AeronResourcesConfig config) {
    this.config = config;

    start
        .doOnTerminate(this::doStart)
        .subscribe(
            avoid -> logger.info("{} has started", this),
            th -> {
              logger.error("Start of {} failed with error: {}", this, th);
              dispose();
            });

    dispose
        .then(doDispose())
        .doFinally(s -> onDispose.onComplete())
        .subscribe(
            avoid -> logger.info("{} closed", this),
            th -> logger.warn("{} closed with error: {}", this, th.toString()));
  }

  /**
   * Starts aeron resources with default config.
   *
   * @return started instance of aeron resources
   */
  public static AeronResources start() {
    return start(AeronResourcesConfig.defaultConfig());
  }

  /**
   * Starts aeron resources with given config.
   *
   * @param config aeron config
   * @return started instance of aeron resources
   */
  public static AeronResources start(AeronResourcesConfig config) {
    AeronResources aeronResources = new AeronResources(config);
    aeronResources.start0();
    return aeronResources;
  }

  private void start0() {
    if (!isDisposed()) {
      start.onComplete();
    }
  }

  private void doStart() {
    MediaDriver.Context mediaContext =
        new MediaDriver.Context()
            .aeronDirectoryName(config.aeronDirectoryName())
            .mtuLength(config.mtuLength())
            .imageLivenessTimeoutNs(config.imageLivenessTimeout().toNanos())
            .dirDeleteOnStart(config.isDirDeleteOnStart());

    mediaDriver = MediaDriver.launchEmbedded(mediaContext);

    Aeron.Context aeronContext = new Aeron.Context();
    String directoryName = mediaDriver.aeronDirectoryName();
    aeronContext.aeronDirectoryName(directoryName);

    aeron = Aeron.connect(aeronContext);

    eventLoopGroup = new AeronEventLoopGroup(config.idleStrategySupplier(), config.numOfWorkers());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteAeronDirectory(aeronContext)));

    logger.info(
        "{} has initialized embedded mediaDriver, aeron directory: {}", this, directoryName);
  }

  public AeronEventLoop nextEventLoop() {
    return eventLoopGroup.next();
  }

  /**
   * Adds and registers new message publication.
   *
   * @param category category
   * @param channel channel
   * @param streamId stream id
   * @param options options
   * @param eventLoop event loop where publocation would be registered
   * @return mono handle of creation and registering of message publication
   */
  public Mono<MessagePublication> messagePublication(
      String category,
      String channel,
      int streamId,
      AeronOptions options,
      AeronEventLoop eventLoop) {

    Publication publication = aeron.addExclusivePublication(channel, streamId);

    MessagePublication messagePublication =
        new MessagePublication(category, publication, options, eventLoop);

    return eventLoop
        .register(messagePublication)
        .doOnError(
            ex -> {
              logger.error(
                  "Failed to register publication: {}, cause: {}",
                  messagePublication,
                  ex.toString());
              if (!publication.isClosed()) {
                publication.close();
              }
            })
        .thenReturn(messagePublication);
  }

  /**
   * Adds control subscription and register it.
   *
   * @param channel channel
   * @param streamId stream id
   * @param subscriber control message subscriber
   * @param eventLoop event loop where to assign control subscription
   * @param availableImageHandler called when {@link Image}s become available for consumption. Null
   *     is valid if no action is to be taken.
   * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null
   *     is valid if no action is to be taken.
   * @return mono handle of creation and registering of control message subscription
   */
  public Mono<MessageSubscription> controlSubscription(
      String category,
      String channel,
      int streamId,
      ControlMessageSubscriber subscriber,
      AeronEventLoop eventLoop,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    return messageSubscription(
        category + "-control",
        channel,
        streamId,
        new ControlFragmentHandler(subscriber),
        eventLoop,
        availableImageHandler,
        unavailableImageHandler);
  }

  /**
   * Adds data subscription and register it.
   *
   * @param channel channel
   * @param streamId stream id
   * @param subscriber data message subscriber
   * @param eventLoop event loop where to assign data subscription
   * @param availableImageHandler called when {@link Image}s become available for consumption. Null
   *     is valid if no action is to be taken.
   * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null
   *     is valid if no action is to be taken.
   * @return mono handle of creation and registering of data message subscription
   */
  public Mono<MessageSubscription> dataSubscription(
      String category,
      String channel,
      int streamId,
      DataMessageSubscriber subscriber,
      AeronEventLoop eventLoop,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    return messageSubscription(
        category + "-data",
        channel,
        streamId,
        new DataFragmentHandler(subscriber),
        eventLoop,
        availableImageHandler,
        unavailableImageHandler);
  }

  @Override
  public void dispose() {
    if (!isDisposed()) {
      dispose.onComplete();
    }
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
          logger.info("{} shutdown initiated", this);

          eventLoopGroup.dispose();

          return eventLoopGroup
              .onDispose()
              .doFinally(
                  s -> {
                    CloseHelper.quietClose(aeron);
                    CloseHelper.quietClose(mediaDriver);
                    Optional.ofNullable(mediaDriver) //
                        .map(MediaDriver::context)
                        .ifPresent(context -> IoUtil.delete(context.aeronDirectory(), true));

                    logger.info("{} shutdown complete", this);
                  });
        });
  }

  private Mono<MessageSubscription> messageSubscription(
      String category,
      String channel,
      int streamId,
      FragmentHandler fragmentHandler,
      AeronEventLoop eventLoop,
      Consumer<Image> availableImageHandler,
      Consumer<Image> unavailableImageHandler) {

    Subscription subscription =
        aeron.addSubscription(
            channel,
            streamId,
            image -> {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Available image on: {}, imageSessionId={}, imageSource={}",
                    AeronUtils.format(category, "sub", channel, streamId),
                    image.sessionId(),
                    image.sourceIdentity());
              }
              if (availableImageHandler != null) {
                availableImageHandler.accept(image);
              }
            },
            image -> {
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Unavailable image on: {}, imageSessionId={}, imageSource={}",
                    category,
                    AeronUtils.format(category, "sub", channel, streamId),
                    image.sessionId(),
                    image.sourceIdentity());
              }
              if (unavailableImageHandler != null) {
                unavailableImageHandler.accept(image);
              }
            });

    MessageSubscription messageSubscription =
        new MessageSubscription(
            category, eventLoop, subscription, new FragmentAssembler(fragmentHandler));

    return eventLoop
        .register(messageSubscription)
        .doOnError(
            ex -> {
              logger.error(
                  "Failed to register subscription: {}, cause: {}",
                  messageSubscription,
                  ex.toString());
              if (!subscription.isClosed()) {
                subscription.close();
              }
            })
        .thenReturn(messageSubscription);
  }

  private void deleteAeronDirectory(Context context) {
    File file = context.aeronDirectory();
    if (file.exists()) {
      IoUtil.delete(file, true);
      logger.debug("Deleted aeron directory {}", file);
    }
  }

  @Override
  public String toString() {
    return "AeronResources@" + Integer.toHexString(System.identityHashCode(this));
  }
}

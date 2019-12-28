package reactor.aeron;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;

public class ImageAgent<T> implements Agent, AeronInbound<T>, Disposable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImageAgent.class);

  private static final AtomicLongFieldUpdater<ImageAgent> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(ImageAgent.class, "requested");

  private static final AtomicReferenceFieldUpdater<ImageAgent, CoreSubscriber>
      DESTINATION_SUBSCRIBER =
          AtomicReferenceFieldUpdater.newUpdater(
              ImageAgent.class, CoreSubscriber.class, "destinationSubscriber");

  private static final CoreSubscriber CANCELLED_SUBSCRIBER = new CancelledSubscriber();

  private static final int FRAGMENT_LIMIT = 10;

  private final FluxReceive inbound = new FluxReceive();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private final Image image;
  private final boolean shouldCloseSubscription;
  private final long stopPosition;

  private final FragmentMapper<T> mapper;
  private final ControlledFragmentHandler fragmentHandler =
      new ControlledFragmentAssembler(new AgentFragmentHandler());

  private volatile long requested;
  private volatile boolean fastPath;
  private long produced;
  private volatile CoreSubscriber<T> destinationSubscriber;
  private Exception ex;

  /**
   * Creates image agent.
   *
   * @param image image
   * @param mapper fragment mapper
   * @param shouldCloseSubscription should the agent close image subscription when the agent is
   *     closed?
   */
  public ImageAgent(Image image, FragmentMapper<T> mapper, boolean shouldCloseSubscription) {
    this(image, mapper, shouldCloseSubscription, Aeron.NULL_VALUE);
  }

  /**
   * Creates image agent.
   *
   * @param image image
   * @param mapper fragment mapper
   * @param shouldCloseSubscription should the agent close image subscription when the agent is
   *     closed?
   * @param stopPosition expected stop position of the given image. A negative value means an
   *     endless stream.
   */
  public ImageAgent(
      Image image, FragmentMapper<T> mapper, boolean shouldCloseSubscription, long stopPosition) {
    this.image = image;
    this.mapper = mapper;
    this.shouldCloseSubscription = shouldCloseSubscription;
    this.stopPosition = stopPosition;
  }

  @Override
  public void onStart() {
    // no-op
  }

  @Override
  public int doWork() {
    if (CANCELLED_SUBSCRIBER.equals(destinationSubscriber)) {
      throw new AgentTerminationException("Subscription is cancelled");
    }
    if (ex != null) {
      if (ex instanceof AgentTerminationException) {
        throw (AgentTerminationException) ex;
      }
      CoreSubscriber destination =
          DESTINATION_SUBSCRIBER.getAndSet(ImageAgent.this, CANCELLED_SUBSCRIBER);
      if (destination != null) {
        destination.onError(ex);
      }
      throw new AgentTerminationException(ex);
    }
    if (image.position() == stopPosition) {
      LOGGER.debug("Image {} achieved specified stop position {}", image.sessionId(), stopPosition);
      throw new AgentTerminationException();
    }
    if (image.isClosed()) {
      CoreSubscriber destination =
          DESTINATION_SUBSCRIBER.getAndSet(ImageAgent.this, CANCELLED_SUBSCRIBER);
      if (destination != null) {
        destination.onError(new AgentTerminationException("Image is closed"));
      }
      throw new AgentTerminationException("Image is closed");
    }
    if (fastPath) {
      return image.controlledPoll(fragmentHandler, FRAGMENT_LIMIT);
    }
    int r = (int) Math.min(requested, FRAGMENT_LIMIT);
    int fragments = 0;
    if (r > 0) {
      fragments = image.controlledPoll(fragmentHandler, r);
      if (produced > 0) {
        Operators.produced(REQUESTED, this, produced);
        produced = 0;
      }
    }
    return fragments;
  }

  @Override
  public void onClose() {
    inbound.cancel();
    LOGGER.debug("Cancelled inbound");
    if (shouldCloseSubscription) {
      CloseHelper.quietClose(image.subscription());
    }
    onDispose.onComplete();
  }

  @Override
  public String roleName() {
    return ImageAgent.class.getName() + ":" + image.sessionId();
  }

  @Override
  public Flux<T> receive() {
    return inbound;
  }

  @Override
  public void dispose() {
    CoreSubscriber destination =
        DESTINATION_SUBSCRIBER.getAndSet(ImageAgent.this, CANCELLED_SUBSCRIBER);
    if (destination != null) {
      destination.onError(new AgentTerminationException("Image Inbound has been disposed"));
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

  private class AgentFragmentHandler implements ControlledFragmentHandler {

    @Override
    public Action onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      try {
        CoreSubscriber<T> destination = ImageAgent.this.destinationSubscriber;
        if (CANCELLED_SUBSCRIBER.equals(destination)) {
          return Action.ABORT;
        }
        T t = mapper.apply(buffer, offset, length, header);
        if (t != null) {
          destination.onNext(t);
          produced++;
        }
      } catch (Exception e) {
        ex = e;
        return Action.ABORT;
      }
      return Action.CONTINUE;
    }
  }

  private class FluxReceive extends Flux<T> implements org.reactivestreams.Subscription {

    @Override
    public void request(long n) {
      if (fastPath) {
        return;
      }
      if (n == Long.MAX_VALUE) {
        fastPath = true;
        requested = Long.MAX_VALUE;
        return;
      }
      Operators.addCap(REQUESTED, ImageAgent.this, n);
    }

    @Override
    public void cancel() {
      CoreSubscriber destination =
          DESTINATION_SUBSCRIBER.getAndSet(ImageAgent.this, CANCELLED_SUBSCRIBER);
      if (destination != null) {
        destination.onComplete();
      }
      LOGGER.debug("Destination subscriber on aeron inbound has been cancelled");
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> destinationSubscriber) {
      boolean result =
          DESTINATION_SUBSCRIBER.compareAndSet(ImageAgent.this, null, destinationSubscriber);
      if (result) {
        destinationSubscriber.onSubscribe(this);
      } else {
        // only subscriber is allowed on receive()
        Operators.error(destinationSubscriber, Exceptions.duplicateOnSubscribeException());
      }
    }
  }

  private static class CancelledSubscriber implements CoreSubscriber {

    @Override
    public void onSubscribe(org.reactivestreams.Subscription s) {
      // no-op
    }

    @Override
    public void onNext(Object o) {
      LOGGER.warn("Received ({}) which will be dropped immediately due cancelled aeron inbound", o);
    }

    @Override
    public void onError(Throwable t) {
      // no-op
    }

    @Override
    public void onComplete() {
      // no-op
    }
  }
}

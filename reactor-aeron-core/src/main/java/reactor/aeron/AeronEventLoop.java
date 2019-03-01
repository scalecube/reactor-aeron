package reactor.aeron;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

final class AeronEventLoop implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronEventLoop.class);

  private final IdleStrategy idleStrategy;

  private final String name;
  private final int workerId; // worker id
  private final int groupId; // event loop group id

  private final Queue<CommandTask> commands = new ConcurrentLinkedQueue<>();
  private final List<AeronResource> resources = new ArrayList<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private final Mono<Worker> workerMono;

  private volatile Thread thread;

  private List<MessagePublication> publications = new ArrayList<>();
  private List<DefaultAeronInbound> inbounds = new ArrayList<>();

  /**
   * Constructor.
   *
   * @param name thread name
   * @param workerId worker id
   * @param groupId id of parent {@link AeronEventLoopGroup}
   * @param idleStrategy {@link IdleStrategy} instance for this event loop
   */
  AeronEventLoop(String name, int workerId, int groupId, IdleStrategy idleStrategy) {
    this.name = name;
    this.workerId = workerId;
    this.groupId = groupId;
    this.idleStrategy = idleStrategy;
    this.workerMono = Mono.fromCallable(this::createWorker).cache();
  }

  private static ThreadFactory defaultThreadFactory(String threadName) {
    return r -> {
      Thread thread = new Thread(r);
      thread.setName(threadName);
      thread.setUncaughtExceptionHandler(
          (t, e) -> logger.error("Uncaught exception occurred: ", e));
      return thread;
    };
  }

  private Worker createWorker() throws Exception {
    final String threadName = String.format("%s-%x-%d", name, groupId, workerId);
    final ThreadFactory threadFactory = defaultThreadFactory(threadName);

    WorkerFlightRecorder flightRecorder = new WorkerFlightRecorder();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("reactor.aeron:name=" + threadName);
    StandardMBean standardMBean = new StandardMBean(flightRecorder, WorkerMBean.class);
    mbeanServer.registerMBean(standardMBean, objectName);

    Worker worker = new Worker(flightRecorder);
    thread = threadFactory.newThread(worker);
    thread.start();

    return worker;
  }

  /**
   * Returns {@code true} if client called this method from within worker thread of {@link
   * AeronEventLoop}, and {@code false} otherwise.
   *
   * @return {@code true} if current thread is worker thread from event loop, and {@code false}
   *     otherwise
   */
  boolean inEventLoop() {
    return thread == Thread.currentThread();
  }

  /**
   * Registers aeron resource in event loop.
   *
   * @param resource aeron resource
   * @return mono result
   */
  <R extends AeronResource> Mono<R> register(R resource) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        resources.add(resource);
                        resetOutboundAndInbound();
                        logger.debug("Registered {}", resource);
                        sink.success(resource);
                      }
                    }));
  }

  /**
   * Disposes resource and remove it from event loop.
   *
   * @param resource aeron resource
   * @return mono result
   */
  Mono<Void> dispose(AeronResource resource) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      resources.remove(resource);
                      resetOutboundAndInbound();
                      logger.debug("Closing {}", resource);
                      Mono.fromRunnable(resource::close)
                          .subscribe(null, sink::error, sink::success);
                    }));
  }

  @Override
  public void dispose() {
    // start disposing worker (if any)
    dispose.onComplete();

    // finish shutdown right away if no worker was created
    if (thread == null) {
      onDispose.onComplete();
    }
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  private Mono<Worker> worker() {
    return workerMono.takeUntilOther(listenUnavailable());
  }

  private <T> Mono<T> command(Consumer<MonoSink<T>> consumer) {
    return Mono.create(sink -> commands.add(new CommandTask<>(sink, consumer)));
  }

  private <T> Mono<T> listenUnavailable() {
    return dispose //
        .map(avoid -> (T) avoid)
        .switchIfEmpty(Mono.error(AeronExceptions::failWithEventLoopUnavailable));
  }

  /**
   * Runnable task for submitting to {@link #commands} queue. For usage details see methods: {@link
   * #register(AeronResource)} and {@link #dispose(AeronResource)}.
   */
  private static class CommandTask<T> implements Runnable {
    private final MonoSink<T> sink;
    private final Consumer<MonoSink<T>> consumer;

    private CommandTask(MonoSink<T> sink, Consumer<MonoSink<T>> consumer) {
      this.sink = sink;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      try {
        consumer.accept(sink);
      } catch (Exception e) {
        logger.error("Exception occurred on CommandTask: ", e);
        sink.error(e);
      }
    }
  }

  /**
   * Runnable event loop worker.
   *
   * <ul>
   *   <li>runs until dispose signal obtained
   *   <li>on run iteration makes progress on: a) commands; b) publications; c) subscriptions
   *   <li>idles on zero progress
   *   <li>collects and reports runtime stats
   * </ul>
   */
  private class Worker implements Runnable {

    private final WorkerFlightRecorder flightRecorder;

    public Worker(WorkerFlightRecorder flightRecorder) {
      this.flightRecorder = flightRecorder;
    }

    @Override
    public void run() {
      flightRecorder.start();

      while (!dispose.isDisposed()) {
        flightRecorder.countTick();

        // Commands
        processCommands();

        // Outbound
        int o = processOutbound();
        flightRecorder.countOutbound(o);

        // Inbound
        int i = processInbound();
        flightRecorder.countInbound(i);

        int workCount = o + i;
        if (workCount < 1) {
          flightRecorder.countIdle();
        } else {
          flightRecorder.countWork(workCount);
        }

        // Reporting
        flightRecorder.tryReport();

        idleStrategy.idle(workCount);
      }

      // Dispose everything
      try {
        processCommands();
        disposeResources();
      } finally {
        onDispose.onComplete();
      }
    }

    private int processInbound() {
      int result = 0;
      //noinspection ForLoopReplaceableByForEach
      for (int i = 0, n = inbounds.size(); i < n; i++) {
        try {
          result += inbounds.get(i).poll();
        } catch (Exception ex) {
          logger.error("Unexpected exception occurred on inbound.poll(): ", ex);
        }
      }
      return result;
    }

    private int processOutbound() {
      int result = 0;
      //noinspection ForLoopReplaceableByForEach
      for (int i = 0, n = publications.size(); i < n; i++) {
        try {
          result += publications.get(i).publish();
        } catch (Exception ex) {
          logger.error("Unexpected exception occurred on publication.publish(): ", ex);
        }
      }
      return result;
    }

    private void processCommands() {
      CommandTask task;
      while ((task = commands.poll()) != null) {
        task.run();
      }
    }
  }

  private void disposeResources() {
    for (AeronResource resource : resources) {
      try {
        resource.close();
      } catch (Exception ex) {
        logger.warn("Exception occurred at closing AeronResource: {}, cause: {}", resource, ex);
      }
    }
    resources.clear();
    publications.clear();
    inbounds.clear();
  }

  private boolean cancelIfDisposed(MonoSink<?> sink) {
    boolean isDisposed = dispose.isDisposed();
    if (isDisposed) {
      sink.error(AeronExceptions.failWithCancel("CommandTask has been cancelled"));
    }
    return isDisposed;
  }

  private void resetOutboundAndInbound() {
    publications = filterResources(MessagePublication.class);
    inbounds = filterResources(DefaultAeronInbound.class);
  }

  private <T> List<T> filterResources(Class<T> clazz) {
    return resources.stream()
        .filter(clazz::isInstance)
        .map(r -> ((T) r))
        .collect(Collectors.toList());
  }
}

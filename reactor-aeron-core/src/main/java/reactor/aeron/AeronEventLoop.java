package reactor.aeron;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
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
  private final List<MessagePublication> publications = new ArrayList<>();
  private final List<MessageSubscription> subscriptions = new ArrayList<>();
  private final List<DefaultAeronInbound> inbounds = new ArrayList<>();

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  private final Mono<Worker> workerMono;

  private volatile Thread thread;

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
    String threadName = String.format("%s-%x-%d", name, groupId, workerId);
    ThreadFactory threadFactory = defaultThreadFactory(threadName);
    Worker worker = new Worker();

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("reactor.aeron:name=" + threadName);
    StandardMBean standardMBean = new StandardMBean(worker, WorkerMBean.class);
    mbeanServer.registerMBean(standardMBean, objectName);

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
   * Registers {@link MessagePublication} in event loop.
   *
   * @param p message publication
   * @return mono result of registration
   */
  Mono<MessagePublication> registerPublication(MessagePublication p) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        publications.add(p);
                        logger.debug("Registered {}", p);
                        sink.success(p);
                      }
                    }));
  }

  /**
   * Registers {@link MessageSubscription} in event loop.
   *
   * @param s message subscription
   * @return mono result of registration
   */
  Mono<MessageSubscription> registerSubscription(MessageSubscription s) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        subscriptions.add(s);
                        logger.debug("Registered {}", s);
                        sink.success(s);
                      }
                    }));
  }

  /**
   * Registers {@link DefaultAeronInbound} in event loop.
   *
   * @param inbound aeron inbound
   * @return mono result of registration
   */
  Mono<DefaultAeronInbound> registerInbound(DefaultAeronInbound inbound) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      if (!cancelIfDisposed(sink)) {
                        inbounds.add(inbound);
                        logger.debug("Registered {}", inbound);
                        sink.success(inbound);
                      }
                    }));
  }

  /**
   * Disposes {@link MessagePublication} and remove it from event loop.
   *
   * @param p message publication
   * @return mono result
   */
  Mono<Void> disposePublication(MessagePublication p) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      publications.remove(p);
                      Mono.fromRunnable(p::close).subscribe(null, sink::error, sink::success);
                    }));
  }

  /**
   * Disposes {@link MessageSubscription} and remove it from event loop.
   *
   * @param s message subscription
   * @return mono result
   */
  Mono<Void> disposeSubscription(MessageSubscription s) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      subscriptions.remove(s);
                      Mono.fromRunnable(s::close).subscribe(null, sink::error, sink::success);
                    }));
  }

  /**
   * Disposes {@link DefaultAeronInbound} and remove it from event loop.
   *
   * @param inbound aeron inbound
   * @return mono result
   */
  Mono<Void> disposeInbound(DefaultAeronInbound inbound) {
    return worker()
        .flatMap(
            worker ->
                command(
                    sink -> {
                      inbounds.remove(inbound);
                      Mono.fromRunnable(inbound::close).subscribe(null, sink::error, sink::success);
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
    //noinspection unchecked
    return dispose //
        .map(avoid -> (T) avoid)
        .switchIfEmpty(Mono.error(AeronExceptions::failWithEventLoopUnavailable));
  }

  /**
   * Runnable task for submitting to {@link #commands} queue. For usage details see methods: {@link
   * #registerPublication(MessagePublication)} and {@link #disposePublication(MessagePublication)}.
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
   * JMX MBean exposer class for event loop worker thread, for {@link Worker}. Contains various
   * runtime stats.
   */
  public interface WorkerMBean {

    long getTicks();

    double getOutboundRate();

    double getInboundRate();

    double getIdleRate();
  }

  /**
   * Runnable event loop worker. Does a following:
   *
   * <ul>
   *   <li>runs until dispose signal obtained
   *   <li>on run iteration makes progress on: a) commands; b) publications; c) subscriptions
   *   <li>idles on negative progress
   * </ul>
   */
  private class Worker implements Runnable, WorkerMBean {

    private static final int REPORT_INTERVAL = 1000;

    // Reporting

    private long ticks;
    private double outboundRate;
    private double inboundRate;
    private double idleRate;

    private long lastTotalTicks;
    private long lastTotalOutbounds;
    private long lastTotalInbounds;
    private long lastTotalIdles;

    @Override
    public long getTicks() {
      return ticks;
    }

    @Override
    public double getOutboundRate() {
      return outboundRate;
    }

    @Override
    public double getInboundRate() {
      return inboundRate;
    }

    @Override
    public double getIdleRate() {
      return idleRate;
    }

    @Override
    public void run() {
      long totalTicks = 0;
      long totalOutbounds = 0; // % out >>
      long totalInbounds = 0; // % receive <<
      long totalIdles = 0; // % idle

      long reportTime = System.currentTimeMillis() + REPORT_INTERVAL;

      // Process commands, publications and subscriptions
      while (!dispose.isDisposed()) {

        totalTicks++;

        // Commands
        processCommands();

        int outCount = processOutbound();
        totalOutbounds += outCount;

        int inCount = processInbound();
        totalInbounds += inCount;

        int workCount = outCount + inCount;
        if (workCount < 1) {
          totalIdles++;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime >= reportTime) {
          reportTime = currentTime + REPORT_INTERVAL;
          processReporting(totalTicks, totalOutbounds, totalInbounds, totalIdles);
        }

        idleStrategy.idle(workCount);
      }

      // Dispose publications, subscriptions, inbounds and commands
      try {
        processCommands();
        disposeInbounds();
        disposeSubscriptions();
        disposePublications();
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
          result += publications.get(i).proceed();
        } catch (Exception ex) {
          logger.error("Unexpected exception occurred on publication.proceed(): ", ex);
        }
      }
      return result;
    }

    private void processReporting(
        long totalTicks, long totalOutbounds, long totalInbounds, long totalIdles) {
      ticks = totalTicks - lastTotalTicks;
      outboundRate = (double) (totalOutbounds - lastTotalOutbounds) / ticks;
      inboundRate = (double) (totalInbounds - lastTotalInbounds) / ticks;
      idleRate = (double) (totalIdles - lastTotalIdles) / ticks;

      lastTotalTicks = totalTicks;
      lastTotalOutbounds = totalOutbounds;
      lastTotalInbounds = totalInbounds;
      lastTotalIdles = totalIdles;
    }

    private void processCommands() {
      CommandTask task;
      while ((task = commands.poll()) != null) {
        task.run();
      }
    }

    private void disposePublications() {
      for (Iterator<MessagePublication> it = publications.iterator(); it.hasNext(); ) {
        MessagePublication p = it.next();
        it.remove();
        try {
          p.close();
        } catch (Exception ex) {
          // no-op
        }
      }
    }

    private void disposeSubscriptions() {
      for (Iterator<MessageSubscription> it = subscriptions.iterator(); it.hasNext(); ) {
        MessageSubscription s = it.next();
        it.remove();
        try {
          s.close();
        } catch (Exception ex) {
          // no-op
        }
      }
    }
  }

  private void disposeInbounds() {
    for (Iterator<DefaultAeronInbound> it = inbounds.iterator(); it.hasNext(); ) {
      DefaultAeronInbound inbound = it.next();
      it.remove();
      try {
        inbound.close();
      } catch (Exception ex) {
        // no-op
      }
    }
  }

  private boolean cancelIfDisposed(MonoSink<?> sink) {
    boolean isDisposed = dispose.isDisposed();
    if (isDisposed) {
      sink.error(AeronExceptions.failWithCancel("CommandTask has cancelled"));
    }
    return isDisposed;
  }
}

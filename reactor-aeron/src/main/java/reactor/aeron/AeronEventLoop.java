package reactor.aeron;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public final class AeronEventLoop implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronEventLoop.class);

  private final IdleStrategy idleStrategy;

  private final String name;

  private final int workerId; // worker id
  private final int groupId; // event loop group id

  private final DynamicCompositeAgent agent;
  private final AgentInvoker agentInvoker;

  private volatile Thread thread;

  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

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

    this.agent = new DynamicCompositeAgent(String.format("%s-%x-%d", name, groupId, workerId));
    this.agentInvoker =
        new AgentInvoker(
            ex -> logger.error("Unexpected exception occurred on invoker: ", ex), null, agent);

    start();
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

  private void start() {
    try {
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
    } catch (Throwable th) {
      throw Exceptions.propagate(th);
    }
  }

  /**
   * Registers agent in event loop.
   *
   * @param resource aeron resource
   */
  public void register(Agent resource) {
    agent.add(resource);
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

    private Worker(WorkerFlightRecorder flightRecorder) {
      this.flightRecorder = flightRecorder;
    }

    @Override
    public void run() {
      flightRecorder.start();
      agentInvoker.start();

      try {
        while (!dispose.isDisposed()) {
          flightRecorder.countTick();

          int workCount = agentInvoker.invoke();

          if (workCount > 0) {
            flightRecorder.countWork(workCount);
          } else {
            flightRecorder.countIdle();
          }

          // Reporting
          flightRecorder.tryReport();

          idleStrategy.idle(workCount);
        }
      } catch (Throwable th) {
        dispose.dispose();
      } finally {
        CloseHelper.close(agentInvoker);
        onDispose.dispose();
      }
    }
  }
}

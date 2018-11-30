package reactor.aeron;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.MonoSink;

final class AeronEventLoop implements OnDisposable {

  private static final Logger logger = LoggerFactory.getLogger(AeronEventLoop.class);

  // Dispose signals
  private final MonoProcessor<Void> dispose = MonoProcessor.create();
  private final MonoProcessor<Void> onDispose = MonoProcessor.create();

  // Worker
  private final Mono<Worker> workerMono;
  private volatile Thread thread;

  // Commands
  private final Queue<CommandTask> commandTasks = new ConcurrentLinkedQueue<>();

  // Senders
  private final List<MessagePublication> messagePublications = new ArrayList<>();

  /**
   * Default constructor.
   *
   * <p>
   */
  AeronEventLoop() {
    workerMono =
        Mono.fromCallable(
                () -> {
                  ThreadFactory threadFactory = defaultThreadFactory();
                  Worker w = new Worker();
                  thread = threadFactory.newThread(w);
                  thread.start();
                  return w;
                })
            .cache();
  }

  boolean inEventLoop() {
    return thread == Thread.currentThread();
  }

  Mono<Void> execute(Consumer<MonoSink<Void>> taskConsumer) {
    return worker().flatMap(w -> command(taskConsumer));
  }

  Mono<Void> register(MessagePublication messagePublication) {
    return worker().flatMap(w -> command(sink -> register(messagePublication, sink)));
  }

  Mono<Void> dispose(MessagePublication messagePublication) {
    return worker().flatMap(w -> command(sink -> dispose(messagePublication, sink)));
  }

  @Override
  public Mono<Void> onDispose() {
    return onDispose;
  }

  @Override
  public void dispose() {
    dispose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onDispose.isDisposed();
  }

  private static ThreadFactory defaultThreadFactory() {
    return r -> {
      Thread thread = new Thread(r);
      thread.setName("aeron-event-loop");
      thread.setUncaughtExceptionHandler(
          (t, e) -> logger.error("Uncaught exception occurred: {}", e, e));
      return thread;
    };
  }

  private Mono<Worker> worker() {
    return workerMono.takeUntilOther(listenDispose());
  }

  private Mono<Void> command(Consumer<MonoSink<Void>> consumer) {
    return Mono.create(sink -> commandTasks.add(new CommandTask(sink, consumer)));
  }

  private <T> Mono<T> listenDispose() {
    //noinspection unchecked
    return dispose //
        .map(avoid -> (T) avoid)
        .switchIfEmpty(Mono.error(Exceptions::failWithRejected));
  }

  private void register(MessagePublication messagePublication, MonoSink<Void> sink) {
    Objects.requireNonNull(messagePublication, "messagePublication must be not null");
    messagePublications.add(messagePublication);
    sink.success();
  }

  private void dispose(MessagePublication messagePublication, MonoSink<Void> sink) {
    messagePublications.removeIf(p -> p == messagePublication);
    Optional.ofNullable(messagePublication).ifPresent(MessagePublication::close);
    sink.success();
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
  private class Worker implements Runnable {

    private final IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

    @Override
    public void run() {
      while (!dispose.isDisposed()) {
        // Process commands
        for (; ; ) {
          CommandTask task = commandTasks.poll();
          if (task == null) {
            break;
          }
          task.run();
        }

        // Process publications
        boolean result = false;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = messagePublications.size(); i < n; i++) {
          result |= messagePublications.get(i).proceed();
        }

        idleStrategy.idle(result ? 1 : 0);
      }

      // Dispose commands
      disposeCommandTasks();

      // Dispose publications
      disposePublications();

      // Emit total dispose
      onDispose.onComplete();
    }

    private void disposeCommandTasks() {
      for (; ; ) {
        CommandTask task = commandTasks.poll();
        if (task == null) {
          break;
        }
        task.cancel();
      }
    }

    private void disposePublications() {
      for (Iterator<MessagePublication> it = messagePublications.iterator(); it.hasNext(); ) {
        MessagePublication messagePublication = it.next();
        try {
          messagePublication.close();
        } catch (Exception ex) {
          logger.warn("Exception occurred on closing {}, cause: {}", messagePublication, ex);
        }
        it.remove();
      }
    }
  }

  /**
   * Runnable task for submitting to {@link #commandTasks} queue. For usage see methods {@link
   * #register(MessagePublication)} and {@link #dispose(MessagePublication)}.
   */
  private static class CommandTask implements Runnable {
    private final MonoSink<Void> sink;
    private final Consumer<MonoSink<Void>> consumer;

    private CommandTask(MonoSink<Void> sink, Consumer<MonoSink<Void>> consumer) {
      this.sink = sink;
      this.consumer = consumer;
    }

    @Override
    public void run() {
      try {
        consumer.accept(sink);
      } catch (Exception ex) {
        logger.warn("Exception occurred on command task: {}", ex);
        sink.error(ex);
      }
    }

    private void cancel() {
      sink.error(Exceptions.failWithCancel());
    }
  }
}
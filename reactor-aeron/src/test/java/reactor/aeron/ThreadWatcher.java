package reactor.aeron;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ThreadWatcher {

  private List<String> beforeThreadNames;

  public ThreadWatcher() {
    this.beforeThreadNames = takeThreadNamesSnapshot();
  }

  public boolean awaitTerminated(long timeoutMillis) throws InterruptedException {
    return awaitTerminated(timeoutMillis, new String[] {});
  }

  /**
   * Await termination.
   *
   * @param timeoutMillis timeout
   * @param excludedPrefixes prefixes to exclude
   * @return tru or false
   * @throws InterruptedException exception
   */
  public boolean awaitTerminated(long timeoutMillis, String... excludedPrefixes)
      throws InterruptedException {
    List<String> liveThreadNames;
    long startTime = System.nanoTime();
    while ((liveThreadNames = getLiveThreadNames(excludedPrefixes)).size() > 0) {
      Thread.sleep(100);

      if (System.nanoTime() - startTime > TimeUnit.MILLISECONDS.toNanos(timeoutMillis)) {
        System.err.println("Ouch! These threads were not terminated: " + liveThreadNames);
        return false;
      }
    }
    return true;
  }

  private List<String> getLiveThreadNames(String[] excludedPrefixes) {
    List<String> afterThreadNames = takeThreadNamesSnapshot();
    afterThreadNames.removeAll(beforeThreadNames);
    return afterThreadNames.stream()
        .filter(
            new Predicate<String>() {
              @Override
              public boolean test(String s) {
                for (String prefix : excludedPrefixes) {
                  if (s.startsWith(prefix)) {
                    return false;
                  }
                }
                return true;
              }
            })
        .collect(Collectors.toList());
  }

  private List<String> takeThreadNamesSnapshot() {
    Thread[] tarray;
    int activeCount;
    int actualCount;
    do {
      activeCount = Thread.activeCount();
      tarray = new Thread[activeCount];
      actualCount = Thread.enumerate(tarray);
    } while (activeCount != actualCount);

    List<String> threadNames = new ArrayList<>();
    for (int i = 0; i < actualCount; i++) {
      threadNames.add(tarray[i].getName());
    }

    return threadNames;
  }
}

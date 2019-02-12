package reactor.aeron;

import reactor.aeron.jmx.WorkerMBean;

public final class WorkerFlightRecorder implements WorkerMBean {

  private static final int REPORT_INTERVAL = 1000;

  private final String name;

  private long reportTime;

  private long ticks;
  private long workCount;
  private long idleCount;
  private double outboundRate;
  private double inboundRate;
  private double idleRate;

  long totalTickCount;
  long totalOutboundCount;
  long totalInboundCount;
  long totalIdleCount;
  long totalWorkCount;

  private long lastTotalTickCount;
  private long lastTotalOutboundCount;
  private long lastTotalInboundCount;
  private long lastTotalIdleCount;
  private long lastTotalWorkCount;

  WorkerFlightRecorder(String name) {
    this.name = name;
  }

  public void start() {
    reportTime = System.currentTimeMillis() + REPORT_INTERVAL;
  }

  /**
   * Make reporting if it's time for it. For details see method: {@link #processReporting(long,
   * long, long, long, long)}
   */
  public void tryReport() {
    long currentTime = System.currentTimeMillis();
    if (currentTime >= reportTime) {
      reportTime = currentTime + REPORT_INTERVAL;
      processReporting(
          totalTickCount, totalOutboundCount, totalInboundCount, totalIdleCount, totalWorkCount);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getTicks() {
    return ticks;
  }

  @Override
  public long getWorkCount() {
    return workCount;
  }

  @Override
  public long getIdleCount() {
    return idleCount;
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

  private void processReporting(
      long totalTickCount,
      long totalOutboundCount,
      long totalInboundCount,
      long totalIdleCount,
      long totalWorkCount) {

    ticks = totalTickCount - lastTotalTickCount;
    workCount = totalWorkCount - lastTotalWorkCount;
    idleCount = totalIdleCount - lastTotalIdleCount;
    outboundRate = (double) (totalOutboundCount - lastTotalOutboundCount) / ticks;
    inboundRate = (double) (totalInboundCount - lastTotalInboundCount) / ticks;
    idleRate = (double) (totalIdleCount - lastTotalIdleCount) / ticks;

    lastTotalTickCount = totalTickCount;
    lastTotalWorkCount = totalWorkCount;
    lastTotalIdleCount = totalIdleCount;
    lastTotalOutboundCount = totalOutboundCount;
    lastTotalInboundCount = totalInboundCount;
  }

  public void countTick() {
    totalTickCount++;
  }

  public void countOutbound(int c) {
    totalOutboundCount += c;
  }

  public void countInbound(int c) {
    totalInboundCount += c;
  }

  public void countIdle() {
    totalIdleCount++;
  }

  public void countWork(int c) {
    totalWorkCount += c;
  }
}

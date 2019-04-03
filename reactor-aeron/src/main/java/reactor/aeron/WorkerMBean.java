package reactor.aeron;

/**
 * JMX MBean exposer class for event loop worker thread (see for details {@link AeronEventLoop}).
 * Contains various runtime stats.
 */
public interface WorkerMBean {

  /**
   * Returns number of ticks per last second.
   *
   * @return number of ticks per last seconds
   */
  long getTicks();

  /**
   * Returns number of work done (counted both outbound and inbound) per last second.
   *
   * @return number of work done per last second.
   */
  long getWorkCount();

  /**
   * Returns number of how many times event loop was idling without progress done.
   *
   * @return number of times being idle
   */
  long getIdleCount();

  /**
   * Returns amount of outbound work done per one tick.
   *
   * @return amount of outbound work done per tick
   */
  double getOutboundRate();

  /**
   * Returns amount of inbound work done per one tick.
   *
   * @return amount of inbound work done per tick
   */
  double getInboundRate();

  /**
   * Returns amount of being idle per one tick.
   *
   * @return amount of being idle per tick
   */
  double getIdleRate();
}

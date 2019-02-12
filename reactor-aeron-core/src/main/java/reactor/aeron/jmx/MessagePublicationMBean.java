package reactor.aeron.jmx;

import java.time.Duration;

/**
 * JMX MBean exposer class for message publication.
 */
public interface MessagePublicationMBean {

  String getName();

  int getSessionId();

  String getPublicationChannel();

  String getEventLoopName();

  int getPublishTasksCount();

  int getWriteLimit();

  Duration getConnectTimeout();

  Duration getBackpressureTimeout();

  Duration getAdminActionTimeout();
}

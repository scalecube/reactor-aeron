package reactor.aeron.jmx;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxUtils {

  private static final Logger logger = LoggerFactory.getLogger(JmxUtils.class);

  /**
   * Registers a worker as an MBean with the MBean server.
   *
   * @param worker worker
   */
  public static <T extends WorkerMBean> void register(T worker) {
    try {
      ObjectName objectName = new ObjectName("reactor.aeron.workers:name=" + worker.getName());
      registerMBean(worker, WorkerMBean.class, objectName);
    } catch (Exception e) {
      logger.warn("Can't register worker {}", worker.getName(), e);
    }
  }

  /**
   * Registers a message publication as an MBean with the MBean server.
   *
   * @param publication message publication
   */
  public static <T extends MessagePublicationMBean> void register(T publication) {
    try {
      ObjectName objectName =
          new ObjectName("reactor.aeron.publications:name=" + publication.getName());
      registerMBean(publication, MessagePublicationMBean.class, objectName);
    } catch (Exception e) {
      logger.warn("Can't register publication.getName() {}", publication, e);
    }
  }

  /**
   * Unregisters a worker from the MBean server.
   *
   * @param worker worker
   */
  public static <T extends WorkerMBean> void unregister(T worker) {
    try {
      ObjectName objectName = new ObjectName("reactor.aeron.workers:name=" + worker.getName());
      registerMBean(worker, WorkerMBean.class, objectName);
    } catch (Exception e) {
      logger.warn("Can't register worker {}", worker.getName(), e);
    }
  }

  /**
   * Unregisters a message publication from the MBean server.
   *
   * @param publication message publication
   */
  public static <T extends MessagePublicationMBean> void unregister(T publication) {
    try {
      ObjectName objectName =
          new ObjectName("reactor.aeron.publications:name=" + publication.getName());
      unregisterMBean(objectName);
    } catch (Exception e) {
      logger.warn("Can't unregister publication {}", publication.getName(), e);
    }
  }

  private static <T> void registerMBean(T mbean, Class<T> mbeanInterface, ObjectName name)
      throws Exception {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    StandardMBean standardMBean = new StandardMBean(mbean, mbeanInterface);
    mbeanServer.registerMBean(standardMBean, name);
  }

  private static void unregisterMBean(ObjectName name) throws Exception {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    mbeanServer.unregisterMBean(name);
  }
}

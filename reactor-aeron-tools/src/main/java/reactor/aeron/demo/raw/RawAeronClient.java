package reactor.aeron.demo.raw;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.WorkerFlightRecorder;
import reactor.aeron.WorkerMBean;
import reactor.aeron.demo.raw.RawAeronResources.MsgPublication;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

abstract class RawAeronClient {

  private static final Logger logger = LoggerFactory.getLogger(RawAeronClient.class);

  private static final int STREAM_ID = 0xcafe0000;

  private static final String address = "localhost";
  private static final int port = 13000;
  private static final int controlPort = 13001;
  private static final ChannelUriStringBuilder outboundChannelBuilder =
      new ChannelUriStringBuilder()
          .endpoint(address + ':' + port)
          .reliable(Boolean.TRUE)
          .media("udp");
  private static final ChannelUriStringBuilder inboundChannelBuilder =
      new ChannelUriStringBuilder()
          .controlEndpoint(address + ':' + controlPort)
          .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
          .reliable(Boolean.TRUE)
          .media("udp");

  private final Aeron aeron;

  private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);
  private final WorkerFlightRecorder flightRecorder;
  private Scheduler scheduler;

  RawAeronClient(Aeron aeron) throws Exception {
    this.aeron = aeron;
    this.scheduler = Schedulers.newSingle(this.toString());
    this.flightRecorder = new WorkerFlightRecorder();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("reactor.aeron:name=" + "server@" + this);
    StandardMBean standardMBean = new StandardMBean(flightRecorder, WorkerMBean.class);
    mbeanServer.registerMBean(standardMBean, objectName);
  }

  final void start() {
    scheduler.schedule(
        () -> {
          String outboundChannel = outboundChannelBuilder.build();

          Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);

          int sessionId = publication.sessionId();

          String inboundChannel =
              inboundChannelBuilder.sessionId(sessionId ^ Integer.MAX_VALUE).build();

          Subscription subscription =
              aeron.addSubscription(
                  inboundChannel,
                  STREAM_ID,
                  this::onClientImageAvailable,
                  this::onClientImageUnavailable);

          MsgPublication msgPublication = new MsgPublication(sessionId, publication);

          flightRecorder.start();

          while (true) {
            flightRecorder.countTick();

            int i = processOutbound(msgPublication);
            flightRecorder.countOutbound(i);

            int j = processInbound(subscription);
            flightRecorder.countInbound(j);

            int workCount = i + j;
            if (workCount < 1) {
              flightRecorder.countIdle();
            } else {
              flightRecorder.countWork(workCount);
            }

            // Reporting
            flightRecorder.tryReport();

            idleStrategy.idle(workCount);
          }
        });
  }

  private void onClientImageAvailable(Image image) {
    logger.debug(
        "onClientImageAvailable: {} {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity());
  }

  private void onClientImageUnavailable(Image image) {
    logger.debug(
        "onClientImageUnavailable: {} {}",
        Integer.toHexString(image.sessionId()),
        image.sourceIdentity());
  }

  int processInbound(Subscription subscription) {
    return 0;
  }

  int processOutbound(MsgPublication msgPublication) {
    return 0;
  }
}

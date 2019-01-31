package reactor.aeron.demo.raw;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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

abstract class RawAeronServer {

  private static final Logger logger = LoggerFactory.getLogger(RawAeronServer.class);

  private static final int STREAM_ID = 0xcafe0000;

  private static final String address = "localhost";
  private static final int port = 13000;
  private static final int controlPort = 13001;
  private static final String acceptorChannel =
      new ChannelUriStringBuilder()
          .endpoint(address + ':' + port)
          .reliable(Boolean.TRUE)
          .media("udp")
          .build();
  private static final ChannelUriStringBuilder outboundChannelBuilder =
      new ChannelUriStringBuilder()
          .controlEndpoint(address + ':' + controlPort)
          .reliable(Boolean.TRUE)
          .media("udp");

  private final Aeron aeron;

  private volatile Subscription acceptSubscription;
  private final List<MsgPublication> publications = new CopyOnWriteArrayList<>();

  private final Scheduler scheduler = Schedulers.newSingle(this.toString());
  private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);
  private final WorkerFlightRecorder flightRecorder;

  RawAeronServer(Aeron aeron) throws Exception {
    this.aeron = aeron;

    this.flightRecorder = new WorkerFlightRecorder();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("reactor.aeron:name=" + "server@" + this);
    StandardMBean standardMBean = new StandardMBean(flightRecorder, WorkerMBean.class);
    mbeanServer.registerMBean(standardMBean, objectName);
  }

  final void start() {
    logger.info("bind on " + acceptorChannel);

    acceptSubscription =
        aeron.addSubscription(
            acceptorChannel,
            STREAM_ID,
            this::onAcceptImageAvailable,
            this::onAcceptImageUnavailable);

    scheduler.schedule(
        () -> {
          flightRecorder.start();

          while (true) {
            flightRecorder.countTick();

            int i = processOutbound(publications);
            flightRecorder.countOutbound(i);

            int j = processInbound(acceptSubscription.images());
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

  private void onAcceptImageAvailable(Image image) {
    int sessionId = image.sessionId();
    String outboundChannel =
        outboundChannelBuilder.sessionId(sessionId ^ Integer.MAX_VALUE).build();

    logger.debug(
        "onImageAvailable: {} {}, create outbound {}",
        image.sessionId(),
        image.sourceIdentity(),
        outboundChannel);

    Schedulers.single()
        .schedule(
            () -> {
              Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);
              publications.add(new MsgPublication(sessionId, publication));
            });
  }

  private void onAcceptImageUnavailable(Image image) {
    int sessionId = image.sessionId();

    logger.debug("onImageUnavailable: {} {}", sessionId, image.sourceIdentity());

    Schedulers.single()
        .schedule(
            () -> {
              publications
                  .stream()
                  .filter(publication -> publication.sessionId() == sessionId)
                  .findFirst()
                  .ifPresent(MsgPublication::close);
              publications.removeIf(msgPublication -> msgPublication.sessionId() == sessionId);
            });
  }

  int processInbound(List<Image> images) {
    return 0;
  }

  int processOutbound(List<MsgPublication> publications) {
    return 0;
  }
}

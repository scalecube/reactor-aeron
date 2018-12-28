package reactor.aeron;

import org.reactivestreams.Subscription;

public interface ControlMessageSubscriber {

  void onSubscription(Subscription subscription);

  void onConnect(
      long connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId);

  void onConnectAck(long connectRequestId, long sessionId, int serverSessionStreamId);
}

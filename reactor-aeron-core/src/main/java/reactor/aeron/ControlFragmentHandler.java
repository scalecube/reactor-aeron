package reactor.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlFragmentHandler implements FragmentHandler {

  private static final Logger logger = LoggerFactory.getLogger(ControlFragmentHandler.class);

  private final ControlMessageSubscriber subscriber;

  public ControlFragmentHandler(ControlMessageSubscriber subscriber) {
    this.subscriber = subscriber;
  }

  @Override
  public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
    int index = offset;

    int msgTypeCode = buffer.getInt(index);
    index += BitUtil.SIZE_OF_INT;

    MessageType messageType = MessageType.getByCode(msgTypeCode);

    switch (messageType) {
      case CONNECT:
        onConnect(buffer, index);
        break;
      case CONNECT_ACK:
        onConnectAck(buffer, index);
        break;
      default:
        logger.error("Unsupported message type: {}", messageType);
    }
  }

  private void onConnect(DirectBuffer buffer, int index) {
    final long connectRequestId = buffer.getLong(index);
    index += BitUtil.SIZE_OF_LONG;

    int clientChannelLength = buffer.getInt(index);
    index += BitUtil.SIZE_OF_INT;
    String clientChannel = buffer.getStringWithoutLengthAscii(index, clientChannelLength);
    index += clientChannelLength;

    int clientControlStreamId = buffer.getInt(index);
    index += BitUtil.SIZE_OF_INT;

    int clientSessionStreamId = buffer.getInt(index);

    subscriber.onConnect(
        connectRequestId, clientChannel, clientControlStreamId, clientSessionStreamId);
  }

  private void onConnectAck(DirectBuffer buffer, int index) {
    long sessionId = buffer.getLong(index);
    index += BitUtil.SIZE_OF_LONG;

    int serverSessionStreamId = buffer.getInt(index);
    index += BitUtil.SIZE_OF_INT;

    long connectRequestId = buffer.getLong(index);

    subscriber.onConnectAck(connectRequestId, sessionId, serverSessionStreamId);
  }
}

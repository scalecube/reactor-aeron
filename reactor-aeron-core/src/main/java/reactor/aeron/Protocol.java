package reactor.aeron;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

/** Protocol. */
public class Protocol {

  /**
   * Factory method for connect body.
   *
   * @param connectRequestId connect request id
   * @param clientChannel client channel
   * @param clientControlStreamId client control stream id
   * @param clientSessionStreamId client session stream id
   * @return byte buffer
   */
  public static ByteBuffer createConnectBody(
      long connectRequestId,
      String clientChannel,
      int clientControlStreamId,
      int clientSessionStreamId) {
    int clientChannelLength = clientChannel.getBytes(StandardCharsets.US_ASCII).length;
    int bytesLength =
        BitUtil.SIZE_OF_INT /*MessageType.CONNECT*/
            + BitUtil.SIZE_OF_LONG /*connectRequestId*/
            + BitUtil.SIZE_OF_INT
            + clientChannelLength /*clientChannel*/
            + BitUtil.SIZE_OF_INT /*clientControlStreamId*/
            + BitUtil.SIZE_OF_INT /*clientSessionStreamId*/;

    byte[] bytes = new byte[bytesLength];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    int index = 0;

    // put MessageType.CONNECT
    buffer.putInt(index, MessageType.CONNECT.getCode());
    index += BitUtil.SIZE_OF_INT;

    // put connectRequestId
    buffer.putLong(index, connectRequestId);
    index += BitUtil.SIZE_OF_LONG;

    // put clientChannel length
    buffer.putInt(index, clientChannelLength);
    index += BitUtil.SIZE_OF_INT;
    buffer.putStringWithoutLengthAscii(index, clientChannel);
    index += clientChannelLength;

    // put clientControlStreamId
    buffer.putInt(index, clientControlStreamId);
    index += BitUtil.SIZE_OF_INT;

    // put clientSessionStreamId
    buffer.putInt(index, clientSessionStreamId);
    index += BitUtil.SIZE_OF_INT;

    return ByteBuffer.wrap(bytes, 0, index);
  }

  /**
   * Factory for connect ack body.
   *
   * @param sessionId session id
   * @param connectRequestId connect request id
   * @param serverSessionStreamId server session stream id
   * @return bytebuffer of connect ack body
   */
  public static ByteBuffer createConnectAckBody(
      long sessionId, long connectRequestId, int serverSessionStreamId) {
    int bytesLength =
        BitUtil.SIZE_OF_INT /*MessageType.CONNECT_ACK*/
            + BitUtil.SIZE_OF_LONG /*sessionId*/
            + BitUtil.SIZE_OF_INT /*serverSessionStreamId*/
            + BitUtil.SIZE_OF_LONG /*connectRequestId*/;

    byte[] bytes = new byte[bytesLength];
    UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    int index = 0;

    // put MessageType.CONNECT_ACK
    buffer.putInt(index, MessageType.CONNECT_ACK.getCode());
    index += BitUtil.SIZE_OF_INT;

    // put sessionId
    buffer.putLong(index, sessionId);
    index += BitUtil.SIZE_OF_LONG;

    // put serverSessionStreamId
    buffer.putInt(index, serverSessionStreamId);
    index += BitUtil.SIZE_OF_INT;

    // put connectRequestId
    buffer.putLong(index, connectRequestId);
    return ByteBuffer.wrap(bytes);
  }
}

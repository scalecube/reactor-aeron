package reactor.aeron;

public enum MessageType {
  CONNECT(1),
  CONNECT_ACK(2);

  private final int code;

  MessageType(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * Returns MessageType by code.
   *
   * @param code code
   * @return MessageType
   */
  public static MessageType getByCode(int code) {
    switch (code) {
      case 1:
        return CONNECT;
      case 2:
        return CONNECT_ACK;
      default:
        throw new IllegalArgumentException("Illegal message type code: " + code);
    }
  }
}

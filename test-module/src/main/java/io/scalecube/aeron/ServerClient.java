package io.scalecube.aeron;

import static io.scalecube.aeron.SimpleServer.STREAM_ID;
import static java.lang.Boolean.TRUE;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

class ServerClient implements AutoCloseable {

  private final int session;
  private final Image image;
  private final Aeron aeron;
  private final UnsafeBuffer buffer;

  private Publication publication; // calculated

  public ServerClient(int session, Image image, Aeron aeron) {
    this.session = session;
    this.image = image;
    this.aeron = aeron;
    this.buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));
  }

  @Override
  public void close() {
    if (publication != null) {
      publication.close();
    }
  }

  public void onReceiveMessage(String message) {
    System.out.println("receive [0x" + Integer.toUnsignedString(this.session) + "]: " + message);

    if (publication == null) {
      // we don't know remote address so we use the given message as a remote address
      initPublication(message);
      return;
    }

    doOnSomeLogic(message);
  }

  private void doOnSomeLogic(String message) {
    // just send echo
    SimpleAeronUtil.sendMessage(this.publication, buffer, message);
  }

  private void initPublication(final String remoteAddress) {

    try {
      String remoteUri =
          new ChannelUriStringBuilder().reliable(TRUE).media("udp").endpoint(remoteAddress).build();

      System.out.println("init publication with remoteUri: " + remoteUri);

      this.publication = this.aeron.addPublication(remoteUri, STREAM_ID);

    } catch (Exception e) {
      System.err.println("client sent malformed initial message: " + remoteAddress);
      e.printStackTrace();
    }
  }
}

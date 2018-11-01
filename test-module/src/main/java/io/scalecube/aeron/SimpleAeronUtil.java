package io.scalecube.aeron;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class SimpleAeronUtil {

  public static Publication addPublication(Aeron aeron, int streamId, String remoteAddress) {
    final String remoteUri =
        new ChannelUriStringBuilder().reliable(TRUE).media("udp").endpoint(remoteAddress).build();
    System.out.println("addPublication with remoteUri: " + remoteUri);
    return aeron.addPublication(remoteUri, streamId);
  }

  public static Subscription addSubscription(Aeron aeron, int streamId, String localAddress) {
    final String localUri =
        new ChannelUriStringBuilder().reliable(TRUE).media("udp").endpoint(localAddress).build();
    System.out.println("addSubscription with localUri: " + localUri);
    return aeron.addSubscription(
        localUri,
        streamId,
        image -> System.out.println("onClientConnected: " + image.sessionId()),
        image -> System.out.println("onClientDisconnected: " + image.sessionId()));
  }

  public static boolean sendMessage(
      final Publication pub, final UnsafeBuffer buffer, final String text) {
    System.out.println("send message: " + text);

    final byte[] value = text.getBytes(UTF_8);
    buffer.putBytes(0, value);

    long result = 0L;
    for (int index = 0; index < 5; ++index) {
      result = pub.offer(buffer, 0, text.length());
      if (result < 0L) {
        try {
          Thread.sleep(100L);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        continue;
      }
      return true;
    }

    System.err.println("could not send, result: " + result);
    return false;
  }

  public static void onParseMessage(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {
    final byte[] buf = new byte[length];
    buffer.getBytes(offset, buf);
    final String str = new String(buf, UTF_8);
    System.out.println("onFragment: " + str);
  }
}

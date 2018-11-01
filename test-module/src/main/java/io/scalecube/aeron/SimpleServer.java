package io.scalecube.aeron;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.logbuffer.Header;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.agrona.DirectBuffer;

public class SimpleServer implements AutoCloseable {

  public static final int STREAM_ID = 0x2044f002;
  private static final String LOCALHOST_ADDRESS = "127.0.0.1:8111";

  private ExecutorService executorService =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread thread = new Thread(r, "server");
            thread.setDaemon(true);
            return thread;
          });

  private final String localAddress;
  private final MediaDriver mediaDriver;
  private final Aeron aeron;
  private final ConcurrentHashMap<Integer, ServerClient> clients;

  public SimpleServer(String localAddress, MediaDriver mediaDriver, Aeron aeron) {
    this.localAddress = localAddress;
    this.mediaDriver = mediaDriver;
    this.aeron = aeron;
    this.clients = new ConcurrentHashMap<>();
  }

  public void start() throws Exception {
    try (Subscription subscription = addSubscription(STREAM_ID)) {
      FragmentAssembler assembler = new FragmentAssembler(this::onReceive);

      while (true) {
        if (subscription.isConnected()) {
          int workCount = subscription.poll(assembler, 8);
        }
        Thread.sleep(100L);
      }
    }
  }

  private void onReceive(
      final DirectBuffer buffer, final int offset, final int length, final Header header) {
    final int session = header.sessionId();

    ServerClient client = clients.get(session);
    if (client == null) {
      System.err.println("Received message from unknown client: " + session);
      return;
    }
    final byte[] buf = new byte[length];
    buffer.getBytes(offset, buf);
    final String message = new String(buf, UTF_8);
    client.onReceiveMessage(message);
  }

  private Subscription addSubscription(int streamId) {
    final String localUri =
        new ChannelUriStringBuilder().reliable(TRUE).media("udp").endpoint(localAddress).build();
    System.out.println("addSubscription with localUri: " + localUri);
    return aeron.addSubscription(
        localUri, streamId, this::onClientConnected, this::onClientDisconnected);
  }

  private void onClientDisconnected(final Image image) {
    final int session = image.sessionId();
    System.out.println(
        "sessionId=" + session + ", onClientDisconnected: " + image.sourceIdentity());
    try (ServerClient client = this.clients.remove(session)) {
      System.out.println(
          "sessionId=" + session + ", onClientDisconnected: closing client " + client);
    } catch (final Exception e) {
      System.err.println(
          "sessionId=" + session + ", onClientDisconnected: failed to close client: ");
      e.printStackTrace();
    }
  }

  private void onClientConnected(final Image image) {
    final int session = image.sessionId();
    System.out.println("sessionId=" + session + ", onClientConnected: " + image.sourceIdentity());
    this.clients.put(session, new ServerClient(session, image, this.aeron));
  }

  public static void main(String[] args) throws Exception {
    Path mediaDir = Paths.get("media-driver-server");

    MediaDriver.Context mediaDriverContext =
        new Context()
            .dirDeleteOnStart(true)
            .aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    Aeron.Context aeronContext =
        new Aeron.Context().aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    try (MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
        Aeron aeron = Aeron.connect(aeronContext);
        SimpleServer server = new SimpleServer(LOCALHOST_ADDRESS, mediaDriver, aeron); ) {

      server.start();

      Thread.currentThread().join();
    }

    //      BackoffIdleStrategy backoffIdleStrategy = new BackoffIdleStrategy(100, 10,
    // TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
    //
    //
    //      executorService.execute(() -> {
    //
    //        while (running.get()) {
    //          int workCount = subscription.poll(new
    // FragmentAssemblerSimple(AeronUtil::onParseMessage), 8);
    //          backoffIdleStrategy.idle(workCount);
    //        }
    //
    //
    //      });
  }

  @Override
  public void close() {
    if (clients != null) {
      this.clients.forEach((id, client) -> client.close());
    }
  }
}

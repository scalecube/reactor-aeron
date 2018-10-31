package io.scalecube.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import reactor.core.publisher.Flux;

import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SimpleServer {

  public static void main(String[] args) throws Exception {

    ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r, "server");
      thread.setDaemon(true);
      return thread;
    });

    Path mediaDir = Paths.get("media-driver");

    int streamId = 0x2044f002;

    MediaDriver.Context mediaDriverContext =
        new Context()
            .dirDeleteOnStart(true)
            .aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    Aeron.Context aeronContext =
        new Aeron.Context().aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    AtomicBoolean running = new AtomicBoolean(true);

    try (MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
        Aeron aeron = Aeron.connect(aeronContext)) {


//      InetSocketAddress localAddress = new InetSocketAddress(8111);

      final String uri =
          new ChannelUriStringBuilder()
              .reliable(TRUE)
              .media("udp")
              .endpoint("localhost:8111")
//              .endpoint(localAddress.toString().replaceFirst("^/", ""))
              .build();

      System.out.println("uri: " + uri);

      Subscription subscription =
          aeron.addSubscription(
              uri,
              streamId,
              image -> System.out.println("onClientConnected: " + image.sessionId()),
              image -> System.out.println("onClientDisconnected: " + image.sessionId()));

      BackoffIdleStrategy backoffIdleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));


      FragmentHandler fragmentHandler = new MyFragmentHandler();

      executorService.execute(() -> {

        while (running.get()) {
          int workCount = subscription.poll(fragmentHandler, 8);
          backoffIdleStrategy.idle(workCount);
        }


      });


      Thread.currentThread().join();


    }
  }

  private static class MyFragmentHandler implements FragmentHandler {

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      final byte[] buf = new byte[length];
      buffer.getBytes(offset, buf);
      final String str = new String(buf, UTF_8);
      System.out.println("onFragment: " + str);
    }
  }
}

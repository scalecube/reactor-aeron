package io.scalecube.aeron;

import static io.scalecube.aeron.SimpleAeronUtil.addPublication;
import static io.scalecube.aeron.SimpleAeronUtil.addSubscription;
import static io.scalecube.aeron.SimpleAeronUtil.sendMessage;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.logbuffer.FragmentHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class SimpleClient {

  private static final int STREAM_ID = 0x2044f002;
  public static final String REMOTE_ADDRESS = "127.0.0.1:8111";
  public static final String LOCAL_ADDRESS = "127.0.0.1:8112";

  public static void main(String[] args) throws Exception {
//    ExecutorService executorService =
//        Executors.newSingleThreadExecutor(
//            r -> {
//              Thread thread = new Thread(r, "client");
//              thread.setDaemon(true);
//              return thread;
//            });

    Path mediaDir = Paths.get("media-driver-client");

    Context mediaDriverContext =
        new Context()
            .dirDeleteOnStart(true)
            .aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    Aeron.Context aeronContext =
        new Aeron.Context().aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    AtomicBoolean running = new AtomicBoolean(true);

    try (MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
        Aeron aeron = Aeron.connect(aeronContext);
        Subscription subscription = addSubscription(aeron, STREAM_ID, LOCAL_ADDRESS);
        Publication publication = addPublication(aeron, STREAM_ID, REMOTE_ADDRESS)) {

      final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));

      final Random random = new Random();

      // Try repeatedly to send an initial message
      while (true) {
        if (publication.isConnected()) {
          if (sendMessage(publication, buffer, LOCAL_ADDRESS)) {
            break;
          }
        }
        Thread.sleep(1000L);
      }

      // Send an infinite stream of random unsigned integers.
      final FragmentHandler assembler = new FragmentAssembler(SimpleAeronUtil::onParseMessage);

      while (true) {
        if (publication.isConnected()) {
          sendMessage(publication, buffer, Integer.toUnsignedString(random.nextInt()));
        }
        if (subscription.isConnected()) {
          subscription.poll(assembler, 10);
        }
        Thread.sleep(1000L);
      }

      //      Thread.currentThread().join();
    }
  }
}

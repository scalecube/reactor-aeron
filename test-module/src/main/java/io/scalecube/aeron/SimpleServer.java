package io.scalecube.aeron;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import reactor.core.publisher.Flux;

public class SimpleServer {

  public static void main(String[] args) {

    Path mediaDir = Paths.get("media-driver");

    MediaDriver.Context mediaDriverContext =
        new Context()
            .dirDeleteOnStart(true)
            .aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    Aeron.Context aeronContext =
        new Aeron.Context().aeronDirectoryName(mediaDir.toAbsolutePath().toString());

    try (MediaDriver mediaDriver = MediaDriver.launch(mediaDriverContext);
        Aeron aeron = Aeron.connect(aeronContext)) {


      Subscription subscription = aeron.addSubscription("rec-0", 1);

      Flux.interval(Duration.ofMillis(100))
          .subscribe($ -> subscription.poll())

      new Thread(() -> {

      })


    }
  }
}

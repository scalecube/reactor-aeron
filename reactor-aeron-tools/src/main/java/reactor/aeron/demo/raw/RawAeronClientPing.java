package reactor.aeron.demo.raw;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.time.Duration;
import org.HdrHistogram.Recorder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.demo.raw.RawAeronResources.MsgPublication;
import reactor.core.publisher.Flux;

public class RawAeronClientPing {

  private static final Logger logger = LoggerFactory.getLogger(RawAeronClientPing.class);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    Aeron aeron = RawAeronResources.start();
    new Client(aeron).start();
  }

  private static class Client extends RawAeronClient {

    private final Recorder histogram;
    private final LongFragmentHandler fragmentHandler;

    Client(Aeron aeron) throws Exception {
      super(aeron);

      // start reporter
      histogram = new Recorder(3600000000000L, 3);

      fragmentHandler = new LongFragmentHandler(histogram);

      Flux.interval(Duration.ofSeconds(1))
          .doOnNext(
              i -> {
                System.out.println("---- PING/PONG HISTO ----");
                histogram
                    .getIntervalHistogram()
                    .outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/PONG HISTO ----");
              })
          .subscribe();
    }

    @Override
    int processOutbound(MsgPublication msgPublication) {
      return msgPublication.proceed(generatePayload());
    }

    @Override
    int processInbound(Subscription subscription) {

      int result = 0;

      for (Image image : subscription.images()) {
        result += image.poll(fragmentHandler, 8);
      }
      return result;
    }

    private DirectBuffer generatePayload() {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(Long.BYTES);
      buffer.putLong(0, System.nanoTime());
      return buffer;
    }

    private static class LongFragmentHandler implements FragmentHandler {

      private final Recorder histogram;

      private LongFragmentHandler(Recorder histogram) {
        this.histogram = histogram;
      }

      @Override
      public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        long start = buffer.getLong(offset);
        long diff = System.nanoTime() - start;
        histogram.recordValue(diff);
      }
    }
  }
}

package reactor.aeron.demo.raw;

import io.aeron.Aeron;
import io.aeron.Image;
import java.util.List;
import java.util.Queue;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.demo.raw.RawAeronResources.MsgPublication;

public class RawAeronServerPong {

  private static final Logger logger = LoggerFactory.getLogger(RawAeronServerPong.class);

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) throws Exception {
    Aeron aeron = RawAeronResources.start();
    new Server(aeron).start();
  }

  private static class Server extends RawAeronServer {

    Queue<DirectBuffer> queue = new OneToOneConcurrentArrayQueue<>(1024 * 1024 * 25);

    Server(Aeron aeron) throws Exception {
      super(aeron);
    }

    @Override
    int processInbound(List<Image> images) {
      int result = 0;
      for (Image image : images) {
        try {
          result +=
              image.poll(
                  (buffer, offset, length, header) ->
                      queue.add(new UnsafeBuffer(buffer, offset, length)),
                  8);
        } catch (Exception ex) {
          logger.error("Unexpected exception occurred on inbound.poll(): ", ex);
        }
      }
      return result;
    }

    @Override
    int processOutbound(List<MsgPublication> publications) {
      int result = 0;
      DirectBuffer buffer = queue.poll();
      if (buffer != null) {
        for (MsgPublication publication : publications) {
          result += publication.proceed(buffer);
        }
      }
      return result;
    }
  }
}

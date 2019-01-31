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
  private static final int QUEUE_CAPACITY = 8192;
  private static final int MAX_POLL_FRAGMENT_LIMIT = 8;
  private static final int MAX_WRITE_LIMIT = 8;

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

    Queue<DirectBuffer> queue = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);

    Server(Aeron aeron) throws Exception {
      super(aeron);
    }

    @Override
    int processInbound(List<Image> images) {
      int result = 0;
      if (queue.size() <= QUEUE_CAPACITY - MAX_POLL_FRAGMENT_LIMIT) {
        for (Image image : images) {
          try {
            result +=
                image.poll(
                    (buffer, offset, length, header) ->
                        queue.add(new UnsafeBuffer(buffer, offset, length)),
                    MAX_POLL_FRAGMENT_LIMIT);
          } catch (Exception ex) {
            logger.error("Unexpected exception occurred on inbound.poll(): ", ex);
          }
        }
      }
      return result;
    }

    @Override
    int processOutbound(List<MsgPublication> publications) {
      int result = 0;
      if (!queue.isEmpty()) {
        for (int i = 0, current; i < MAX_WRITE_LIMIT; i++) {
          DirectBuffer buffer = queue.peek();
          current = 0;
          if (buffer != null) {
            for (MsgPublication publication : publications) {
              current += publication.proceed(buffer);
            }
          }
          if (current < 1) {
            break;
          }
          result += current;
        }
      }
      return result;
    }
  }
}

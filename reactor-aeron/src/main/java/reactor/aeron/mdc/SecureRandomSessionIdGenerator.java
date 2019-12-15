package reactor.aeron.mdc;

import io.aeron.driver.MediaDriver.Context;
import java.security.SecureRandom;
import java.util.function.Supplier;

/**
 * Session id generator (in the range {@code 0..Int.MAX_VALUE}) based on {@link SecureRandom}.
 *
 * <p>NOTE: this session id generator aligns with defaults (that comes from {@link AeronResources}
 * object) for {@link Context#publicationReservedSessionIdLow()} and {@link
 * Context#publicationReservedSessionIdHigh()}.
 */
public final class SecureRandomSessionIdGenerator implements Supplier<Integer> {

  private final SecureRandom random = new SecureRandom();

  @Override
  public Integer get() {
    return random.nextInt(Integer.MAX_VALUE);
  }
}

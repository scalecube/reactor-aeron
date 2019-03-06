package reactor.aeron;

import io.aeron.ChannelUriStringBuilder;
import java.util.function.UnaryOperator;

/**
 * Immutable wrapper of {@link ChannelUriStringBuilder}. See methods: {@link #builder()}, {@link
 * #asString()} and {@link #uri(UnaryOperator)}.
 */
public final class AeronChannelUriString {

  /**
   * Source builder. {@link ChannelUriStringBuilder} is mutable, hence copy will be created each
   * time modification is performed on it.
   */
  private final ChannelUriStringBuilder builder =
      new ChannelUriStringBuilder().media("udp").reliable(true);

  public AeronChannelUriString() {}

  private AeronChannelUriString(ChannelUriStringBuilder other) {
    builder
        .endpoint(other.endpoint())
        .controlEndpoint(other.controlEndpoint())
        .mtu(other.mtu())
        .controlMode(other.controlMode())
        .sessionId(other.sessionId())
        .tags(other.tags())
        .isSessionIdTagged(other.isSessionIdTagged())
        .media(other.media())
        .reliable(other.reliable())
        .ttl(other.ttl())
        .initialTermId(other.initialTermId())
        .termOffset(other.termOffset())
        .termLength(other.termLength())
        .termId(other.termId())
        .linger(other.linger())
        .networkInterface(other.networkInterface());
  }

  /**
   * Returns copy of this builder.
   *
   * @return new {@code ChannelUriStringBuilder} instance
   */
  public ChannelUriStringBuilder builder() {
    return new AeronChannelUriString(builder).builder;
  }

  /**
   * Returns result of {@link ChannelUriStringBuilder#build()}.
   *
   * @return aeron channel uri string
   */
  public String asString() {
    return builder().build();
  }

  /**
   * Applies modifier and produces new {@code AeronChannelUriString} object.
   *
   * @param o modifier operator
   * @return new {@code AeronChannelUriString} object
   */
  public AeronChannelUriString uri(UnaryOperator<ChannelUriStringBuilder> o) {
    return new AeronChannelUriString(o.apply(builder()));
  }

  @Override
  public String toString() {
    return "AeronChannelUriString{" + asString() + "}";
  }
}

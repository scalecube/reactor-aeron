package reactor.aeron;

import io.aeron.Image;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

/**
 * Immutable wrapper around options for full-duplex aeron <i>connection</i> between client and
 * server. Note, it's mandatory to set {@code resources}, {@code inboundUri} and {@code
 * outboundUri}, everything rest may come with defaults.
 */
public final class AeronOptions {

  private AeronResources resources;
  private Function<? super AeronConnection, ? extends Publisher<Void>> handler;
  private AeronChannelUriString inboundUri = new AeronChannelUriString();
  private Integer inboundStreamId;
  private AeronChannelUriString outboundUri = new AeronChannelUriString();
  private Integer outboundStreamId;
  private Duration connectTimeout = Duration.ofSeconds(5);
  private int connectRetryCount = 3;
  private Duration backpressureTimeout = Duration.ofSeconds(5);
  private Duration adminActionTimeout = Duration.ofSeconds(5);
  private Supplier<Integer> sessionIdGenerator = new SecureRandomSessionIdGenerator();
  private Consumer<Image> onImageAvailable =
      image -> {
        // no-op
      };
  private Consumer<Image> onImageUnavailable =
      image -> {
        // no-op
      };

  public AeronOptions() {}

  AeronOptions(AeronOptions other) {
    this.resources = other.resources;
    this.handler = other.handler;
    this.inboundUri = other.inboundUri;
    this.inboundStreamId = other.inboundStreamId;
    this.outboundUri = other.outboundUri;
    this.outboundStreamId = other.outboundStreamId;
    this.connectTimeout = other.connectTimeout;
    this.backpressureTimeout = other.backpressureTimeout;
    this.adminActionTimeout = other.adminActionTimeout;
    this.sessionIdGenerator = other.sessionIdGenerator;
    this.connectRetryCount = other.connectRetryCount;
    this.onImageAvailable = other.onImageAvailable;
    this.onImageUnavailable = other.onImageUnavailable;
  }

  public AeronResources resources() {
    return resources;
  }

  public AeronOptions resources(AeronResources resources) {
    return set(s -> s.resources = resources);
  }

  public Function<? super AeronConnection, ? extends Publisher<Void>> handler() {
    return handler;
  }

  public AeronOptions handler(
      Function<? super AeronConnection, ? extends Publisher<Void>> handler) {
    return set(s -> s.handler = handler);
  }

  public AeronChannelUriString inboundUri() {
    return inboundUri;
  }

  public AeronOptions inboundUri(AeronChannelUriString inboundUri) {
    return set(s -> s.inboundUri = inboundUri);
  }

  public Integer inboundStreamId() {
    return inboundStreamId;
  }

  public AeronOptions inboundStreamId(Integer inboundStreamId) {
    return set(s -> s.inboundStreamId = inboundStreamId);
  }

  public AeronChannelUriString outboundUri() {
    return outboundUri;
  }

  public AeronOptions outboundUri(AeronChannelUriString outboundUri) {
    return set(s -> s.outboundUri = outboundUri);
  }

  public Integer outboundStreamId() {
    return outboundStreamId;
  }

  public AeronOptions outboundStreamId(Integer outboundStreamId) {
    return set(s -> s.outboundStreamId = outboundStreamId);
  }

  public Duration connectTimeout() {
    return connectTimeout;
  }

  public AeronOptions connectTimeout(Duration connectTimeout) {
    return set(s -> s.connectTimeout = connectTimeout);
  }

  public int connectRetryCount() {
    return connectRetryCount;
  }

  public AeronOptions connectRetryCount(int connectRetryCount) {
    return set(s -> s.connectRetryCount = connectRetryCount);
  }

  public Duration backpressureTimeout() {
    return backpressureTimeout;
  }

  public AeronOptions backpressureTimeout(Duration backpressureTimeout) {
    return set(s -> s.backpressureTimeout = backpressureTimeout);
  }

  public Duration adminActionTimeout() {
    return adminActionTimeout;
  }

  public AeronOptions adminActionTimeout(Duration adminActionTimeout) {
    return set(s -> s.adminActionTimeout = adminActionTimeout);
  }

  public Supplier<Integer> sessionIdGenerator() {
    return sessionIdGenerator;
  }

  public AeronOptions sessionIdGenerator(Supplier<Integer> sessionIdGenerator) {
    return set(s -> s.sessionIdGenerator = sessionIdGenerator);
  }

  public Consumer<Image> onImageAvailable() {
    return onImageAvailable;
  }

  public AeronOptions onImageAvailable(Consumer<Image> onImageAvailable) {
    return set(s -> s.onImageAvailable = s.onImageAvailable.andThen(onImageAvailable));
  }

  public Consumer<Image> onImageUnavailable() {
    return onImageUnavailable;
  }

  public AeronOptions onImageUnavailable(Consumer<Image> onImageUnavailable) {
    return set(s -> s.onImageUnavailable = s.onImageUnavailable.andThen(onImageUnavailable));
  }

  private AeronOptions set(Consumer<AeronOptions> c) {
    AeronOptions s = new AeronOptions(this);
    c.accept(s);
    return s;
  }
}

package reactor.aeron.demo;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.aeron.buffer.BufferSlab;
import reactor.aeron.buffer.BufferSlice;

@State(Scope.Thread)
@Fork(value = 2)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BufferSlabBenchmark {

  UnsafeBuffer underlying = new UnsafeBuffer(ByteBuffer.allocateDirect(5 + 1024 + 5 + 1024));
  BufferSlab slab = new BufferSlab(underlying);

  @Setup
  public void setUp() {
    slab.allocate(1024); // 5 + 1024
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void newBufferSlice(Blackhole blackhole) {
    blackhole.consume(new BufferSlice(underlying, 0, 32));
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void allocate(Blackhole blackhole) {
    BufferSlice bufferSlice = slab.allocate(1024);
    if (bufferSlice == null) {
      throw new NullPointerException();
    }
    blackhole.consume(bufferSlice);
    bufferSlice.release();
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}

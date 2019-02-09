package reactor.aeron.buffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agrona.concurrent.UnsafeBuffer;

public class BufferPool {

  private final int slabCapacity;
  private final List<BufferSlab> slabs;

  public BufferPool(int slabCapacity) {
    this.slabCapacity = slabCapacity;
    BufferSlab slab = new BufferSlab(new UnsafeBuffer(ByteBuffer.allocateDirect(slabCapacity)));
    this.slabs = new CopyOnWriteArrayList<>();
    slabs.add(slab);
  }

  BufferSlice allocate(int size) {
    for (BufferSlab slab : slabs) {
      BufferSlice slice = slab.allocate(size);
      if (slice != null) {
        return slice;
      }
    }
    BufferSlab slab = new BufferSlab(new UnsafeBuffer(ByteBuffer.allocateDirect(slabCapacity)));
    slabs.add(slab);

    System.err.println(" created new slab,  " + slabs.size());
    return slab.allocate(size);
  }
}

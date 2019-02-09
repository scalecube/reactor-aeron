package reactor.aeron.buffer;

import org.agrona.concurrent.UnsafeBuffer;

public class BufferSlab {

  private final UnsafeBuffer underlying;

  volatile int readIndex;
  volatile int writeIndex;

  public BufferSlab(UnsafeBuffer underlying) {
    this.underlying = underlying;
  }

  public BufferSlice allocate(int size /*without headers*/) {
    int wIndex = this.writeIndex;
    int rLimit = underlying.capacity() - wIndex;
    if (rLimit >= size) {
      int currentOffset = wIndex + size;
      // todo thread safe update
      this.writeIndex = currentOffset;
      return slice(wIndex, size + BufferSlice.HEADER_OFFSET);
    }

    // right limit is not enough, so try to look for appropriate size from beginning

    int currentOffset = 0;
    int availableBytes = 0;

    while (isReleased(currentOffset)) {
      availableBytes += msgLength(currentOffset) + BufferSlice.HEADER_OFFSET;
      if (availableBytes >= size + BufferSlice.HEADER_OFFSET) {
        this.writeIndex = availableBytes + BufferSlice.HEADER_OFFSET;
        return slice(currentOffset, size);
      }
      currentOffset += availableBytes + BufferSlice.HEADER_OFFSET;
    }
    return null;
  }

  private int msgLength(int offset) {
    return underlying.getInt(offset + BufferSlice.FREE_FLAG_OFFSET);
  }

  private boolean isReleased(int offset) {
    return underlying.getByte(offset) == 0;
  }

  private BufferSlice slice(int offset, int size) {
    underlying.putByte(offset, (byte) 1);
    underlying.putInt(offset + BufferSlice.FREE_FLAG_OFFSET, size);
    return new BufferSlice(underlying, offset, size);
  }
}

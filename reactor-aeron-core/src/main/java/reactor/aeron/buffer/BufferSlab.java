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
    int rIndex = this.readIndex;

    if (wIndex == rIndex && !isReleased(rIndex)) {
      return null;
    }

    int availableBytes = wIndex >= rIndex ? underlying.capacity() - wIndex : rIndex - wIndex;
    if (availableBytes >= size) {
      int currentOffset = wIndex + size;
      // todo thread safe update
      this.writeIndex = currentOffset;
      return slice(wIndex, size + BufferSlice.HEADER_OFFSET);
    }

    // right limit is not enough, so try to look for appropriate size from beginning

    int currentOffset = 0;

    int rightLimit = underlying.capacity() - BufferSlice.HEADER_OFFSET;
    while (currentOffset < rightLimit && isReleased(currentOffset)) {
      int nextOffset = nextOffset(currentOffset);
      availableBytes = nextOffset - currentOffset;
      if (availableBytes >= size + BufferSlice.HEADER_OFFSET) {
        this.writeIndex = size + BufferSlice.HEADER_OFFSET;
        return slice(currentOffset, size);
      }
      currentOffset = nextOffset;
    }
    return null;
  }

  private int nextOffset(int offset) {
    return offset + BufferSlice.HEADER_OFFSET + msgLength(offset);
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

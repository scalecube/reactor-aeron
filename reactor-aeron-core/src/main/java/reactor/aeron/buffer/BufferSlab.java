package reactor.aeron.buffer;

import org.agrona.concurrent.UnsafeBuffer;

public class BufferSlab {

  private final UnsafeBuffer underlying;

  private int readIndex;
  private int writeIndex;

  public BufferSlab(UnsafeBuffer underlying) {
    this.underlying = underlying;
    this.writeIndex = 0;
    this.readIndex = 0;
    underlying.putByteVolatile(writeIndex, (byte) 0);
    underlying.putIntVolatile(writeIndex + BufferSlice.FREE_FLAG_OFFSET, underlying.capacity());
  }

  public BufferSlice allocate(int size /*without headers*/) {
    final int fullLength = size + BufferSlice.HEADER_OFFSET;
    int wIndex = writeIndex;
    int rIndex = readIndex;

    while (isReleased(wIndex)) {
      if (rIndex > wIndex) {
        // not enough => move rIndex until not enough and then if not enough again => null
        //  ---w-----r--
        //  ---w------r-
        //  ---w-------r
        //  r--w--------
        int availableBytes = rIndex - wIndex;
        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
          this.readIndex = rIndex;
          return slice(wIndex, fullLength);
        }

        if (!isReleased(rIndex)) {
          return null;
        }

        boolean finish = true;
        while (isReleased(rIndex)) {
          int nextOffset = nextOffset(rIndex);
          if (nextOffset < 0) {
            rIndex = 0;
            continue;
          }
          if (nextOffset == this.writeIndex) {
            wIndex = 0;
            rIndex = 0;

            finish = false; // todo continue
            break;
          }
          rIndex = nextOffset;

          if (rIndex > wIndex) {
            availableBytes = rIndex - wIndex;
          }
          if (wIndex > rIndex) {
            availableBytes = underlying.capacity() - wIndex;
          }

          if (availableBytes >= fullLength) {
            this.writeIndex = wIndex + fullLength;
            this.readIndex = rIndex;
            if (this.writeIndex == underlying.capacity()) {
              this.writeIndex = 0;
            }
            return slice(wIndex, fullLength);
          }
        }

        if (finish) {
          this.readIndex = rIndex;
          return null;
        }
      }

      if (wIndex > rIndex) {
        // not enough => change wIndex = 0, try again and if not enough again => null
        //  ------r---w--
        //  ------r----w-
        //  ------r-----w
        //  w-----r------
        int availableBytes = underlying.capacity() - wIndex;
        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
          this.readIndex = rIndex;
          if (this.writeIndex == underlying.capacity()) {
            this.writeIndex = 0;
          }
          return slice(wIndex, fullLength);
        }

        wIndex = 0;
        // todo continue;
      }

      if (wIndex == 0 && rIndex == 0) {
        //  b------------
        int availableBytes = underlying.capacity();
        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
          this.readIndex = rIndex;
          return slice(wIndex, fullLength);
        }
        return null;
      }

      if (wIndex == rIndex) {
        // not enough => change wIndex = 0, try again and if not enough again => null
        //  ------b------
        //  w-----r------
        //  b------------
        this.writeIndex = 0;
        this.readIndex = 0;
        int availableBytes = underlying.capacity();
        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
          return slice(wIndex, fullLength);
        }
        return null;
      }
    }

    this.readIndex = rIndex;
    return null;
  }

  /**
   * NOTE only for readIndex!!! it will return the same offset if the next offset equals
   * underlying.capacity() it will return readIndex if the next offset equals readIndex it will
   * return -1 if the next offset exceeds underlying.capacity()
   *
   * @param offset current offset
   * @return next offset
   */
  private int nextOffset(int offset) {
    int offsetAndHeaders = offset + BufferSlice.HEADER_OFFSET;
    if (offsetAndHeaders >= readIndex) {
      return readIndex;
    }
    if (offsetAndHeaders >= underlying.capacity()) {
      return -1;
    }
    int nextOffset = offsetAndHeaders + msgLength(offset);
    return nextOffset == underlying.capacity() ? offset : nextOffset;
  }

  private int msgLength(int offset) {
    //    if (underlying.capacity() > offset + BufferSlice.FREE_FLAG_OFFSET)
    return underlying.getInt(offset + BufferSlice.FREE_FLAG_OFFSET);
  }

  private boolean isReleased(int offset) {
    return underlying.getByte(offset) == 0;
  }

  private BufferSlice slice(int offset, int size /*without headers*/) {
    underlying.putByte(offset, (byte) 1);
    underlying.putInt(offset + BufferSlice.FREE_FLAG_OFFSET, size);
    return new BufferSlice(underlying, offset, size);
  }
}

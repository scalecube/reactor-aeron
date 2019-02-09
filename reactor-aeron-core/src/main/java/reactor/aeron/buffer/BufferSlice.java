package reactor.aeron.buffer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import org.agrona.concurrent.UnsafeBuffer;


/**
 * 0/1    - byte, free or not
 * 321    - int, length
 * ...    - bytes, message
 *
 * 0/1    - byte, free or not
 * 213    - int, length
 * ...    - bytes, message
 *
 * 0/1    - byte, free or not
 * 321    - int, length
 * ...    - bytes, message
 *
 *
 *
 *
 */
public class BufferSlice {

  public static final int FREE_FLAG_OFFSET = Byte.BYTES;
  public static final int LENGTH_OFFSET = Integer.BYTES;
  public static final int HEADER_OFFSET = FREE_FLAG_OFFSET + LENGTH_OFFSET;

  private final UnsafeBuffer underlying;
  private final int offset;
  private final int length;

  public BufferSlice(UnsafeBuffer underlying, int offset, int length) {
    this.underlying = underlying;
    this.offset = offset;
    this.length = length;
  }

  public void release() {
    underlying.putByteVolatile(0, (byte) 0);
  }

  public int offset() {
    return offset + HEADER_OFFSET;
  }

  public int capacity() {
    return length - HEADER_OFFSET;
  }

  public void putBytes(int index, ByteBuffer srcBuffer, int srcIndex, int length) {
    underlying.putBytes(offset() + index, srcBuffer, srcIndex, length);
  }

  public int putStringUtf8(int index, String value) {
    return underlying.putStringUtf8(offset() + index, value);
  }

  public String getStringUtf8(int index, int length) {
    return underlying.getStringUtf8(offset() + index, length);
  }

  public void getBytes(int index, ByteBuffer dstBuffer, int dstOffset, int length) {
    underlying.getBytes(offset() + index, dstBuffer, dstOffset, length);
  }



  void print() {
    System.out.println("Slice offset = " + offset);
    System.out.println("Slice length = " + length);
//    System.out.println("Slice all = " + underlying.getStringUtf8(offset, length));
//    System.out.println("Slice msg free = " + underlying.getByte(offset));
//    System.out.println("Slice msg length = " + underlying.getInt(offset + FREE_FLAG_OFFSET));
    System.out.println("Slice msg = " + underlying.getStringUtf8(offset + HEADER_OFFSET, length - HEADER_OFFSET));

//    byte[] bytes = new byte[length];
//    underlying.getBytes(offset + HEADER_OFFSET, bytes);
//
//    System.out.println("Slice bytes msg = " + new String(bytes, UTF_8));
  }
}

package david.lu.indexing.pojo;

import lombok.Builder;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * <pre>
 * This is the last block of index file populate [data size] times. The sorting order is based {@link IndexNode} order.
 *  hash            4 bytes
 *  source offset   8 bytes
 *  source length   4 bytes
 *  </pre>
 */
@Builder
@Getter
public class IndexTunnel {
    public final static int SIZE = 16;
    /**
     * hash of source block
     */
    private int hash;
    /**
     * offset of source block
     */
    private long offset;
    /**
     * length of source block
     */
    private int length;

    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(SIZE)
            .putInt(hash)
            .putLong(offset)
            .putInt(length);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static IndexTunnel fromByteBuffer(ByteBuffer byteBuffer, int index) {
        byteBuffer.position(index * SIZE);
        return builder()
            .hash(byteBuffer.getInt())
            .offset(byteBuffer.getLong())
            .length(byteBuffer.getInt())
            .build();
    }
}

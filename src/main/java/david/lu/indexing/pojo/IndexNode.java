package david.lu.indexing.pojo;

import lombok.Builder;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * <pre>
 * This is the second block of index file populate {@link IndexHeader#capacity} times. Similar as {@link java.util.HashMap} to get corresponding {@link IndexNode} by hash and {@link IndexHeader#capacity}. It's mapped to multi IndexTunnel
 *  collision   1 byte
 *  offset      8 bytes
 *  </pre>
 */
@Builder
@Getter
public class IndexNode {
    public final static int SIZE = 9;
    /**
     * collision defined as {@link java.util.HashMap}
     */
    private byte collision;
    /**
     * timestamp of index created
     */
    private long offset;

    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(SIZE)
            .put(collision)
            .putLong(offset);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static IndexNode fromByteBuffer(ByteBuffer byteBuffer) {
        return builder()
            .collision(byteBuffer.get())
            .offset(byteBuffer.getLong())
            .build();
    }
}

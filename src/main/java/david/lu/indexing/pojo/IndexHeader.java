package david.lu.indexing.pojo;

import lombok.Builder;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * <pre>
 * This is the first block of index file. It's a generic description, including following:
 *  create time             8 bytes
 *  data capacity           4 bytes
 *  data size               8 bytes
 *  max collision           1 byte
 *  </pre>
 */
@Builder
@Getter
public class IndexHeader {
    public final static int SIZE = 21;
    /**
     * timestamp of index created
     */
    private long createTime;
    /**
     * total data size
     */
    private long size;
    /**
     * capacity of hashing
     */
    private int capacity;
    /**
     * max hash collision
     */
    private byte maxCollision;

    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(SIZE)
            .putLong(createTime)
            .putLong(size)
            .putInt(capacity)
            .put(maxCollision);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static IndexHeader fromByteBuffer(ByteBuffer byteBuffer) {
        return builder()
            .createTime(byteBuffer.getLong())
            .size(byteBuffer.getLong())
            .capacity(byteBuffer.getInt())
            .maxCollision(byteBuffer.get())
            .build();
    }
}

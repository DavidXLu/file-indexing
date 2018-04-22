package david.lu.indexing.utils;

import david.lu.indexing.pojo.IndexHeader;
import david.lu.indexing.pojo.IndexNode;
import david.lu.indexing.pojo.IndexTunnel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;

import static lombok.AccessLevel.NONE;

@NoArgsConstructor(access = NONE)
public final class IndexUtils {
    public final static int DEFAULT_CAPACITY = 0xFF;
    public final static int DEFAULT_CHANNELS = 5;
    public static int getCapacity(int size) {
        if (size > (Integer.MAX_VALUE >> 1)) {
            return Integer.MAX_VALUE;
        }
        int target = size << 1;
        int capacity = DEFAULT_CAPACITY + 1;
        while (target > capacity) {
            capacity <<= 1;
        }
        return capacity - 1;
    }
    public static int getIndex(int hash, int capacity) {
        int index = hash & capacity;
        return index < capacity ? index : 0;
    }
    public static int appendTunnel(List<IndexTunnel>[] nodes, IndexTunnel indexTunnel, int capacity) {
        int index = getIndex(indexTunnel.getHash(), capacity);
        if (nodes[index] == null) {
            nodes[index] = new ArrayList<>();
        }
        nodes[index].add(indexTunnel);
        return nodes[index].size();
    }
    public static String formatTime(long time) {
        return String.format(
                "%d min, %d sec, %d ms",
                time / 60000,
                time / 1000 % 60,
                time % 1000
        );
    }
    public static int formatToMillions(long value) {
        return (int) value / (1 << 21);
    }
    public static ByteBuffer byteToByteBuffer(byte value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1).put(value);
        byteBuffer.flip();
        return byteBuffer;
    }
    public static ByteBuffer intToByteBuffer(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4).putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
    public static ByteBuffer longToByteBuffer(long value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8).putLong(value);
        byteBuffer.flip();
        return byteBuffer;
    }
    public static byte intToByte(int value) {
        byte val = (byte) value;
        if ((val & 0xFF) != value) {
            throw new InputMismatchException(String.format("Integer value[%d] is out of Byte range.", value));
        };
        return val;
    }
    public static long getNodeOffset(int index) {
        long offset = index;
        offset *= IndexNode.SIZE;
        offset += IndexHeader.SIZE;
        return offset;
    }
    public static long getFirstTunnelOffset(int capacity) {
        long firstTunnelOffset = capacity;
        firstTunnelOffset *= IndexNode.SIZE;
        firstTunnelOffset += IndexHeader.SIZE;
        return firstTunnelOffset;
    }

    public static long getTunnelOffset(long firstTunnelIndex, long index) {
        long offset = index;
        offset *= IndexTunnel.SIZE;
        offset += firstTunnelIndex;
        return offset;
    }

    public static ByteBuffer loadByteBuffer(FileChannel fileChannel, long offset, int size) throws IOException {
        ByteBuffer block = ByteBuffer.allocate(size);
        fileChannel.read(block, offset);
        block.flip();
        return block;
    }
}

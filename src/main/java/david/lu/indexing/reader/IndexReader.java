package david.lu.indexing.reader;

import david.lu.indexing.pojo.IndexHeader;
import david.lu.indexing.pojo.IndexNode;
import david.lu.indexing.pojo.IndexTunnel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static david.lu.indexing.utils.IndexUtils.DEFAULT_CHANNELS;
import static david.lu.indexing.utils.IndexUtils.getFirstTunnelOffset;
import static david.lu.indexing.utils.IndexUtils.getIndex;
import static david.lu.indexing.utils.IndexUtils.getNodeOffset;
import static david.lu.indexing.utils.IndexUtils.getTunnelOffset;
import static david.lu.indexing.utils.IndexUtils.loadByteBuffer;
import static org.apache.commons.lang3.RandomUtils.nextInt;

@Slf4j
public class IndexReader {
    private FileChannel[] sourceFileChannels;
    private FileChannel[] indexFileChannels;
    private int capacity;
    private long firstTunnelIndex;

    private IndexReader(int channels, File sourceFilePath, File indexFilePath) throws IOException {
        sourceFileChannels = new FileChannel[channels];
        indexFileChannels = new FileChannel[channels];
        for (int i = 0; i < channels; i ++) {
            sourceFileChannels[i] = new RandomAccessFile(sourceFilePath, "r").getChannel();
            indexFileChannels[i] = new RandomAccessFile(indexFilePath, "r").getChannel();
        }
        IndexHeader indexHeader = IndexHeader.fromByteBuffer(
            loadByteBuffer(getRandomIndexFileChannel(), 0, IndexHeader.SIZE)
        );
        capacity = indexHeader.getCapacity();
        firstTunnelIndex = getFirstTunnelOffset(capacity);
        log.debug(
            "Index time: {}, total: {}, capacity: {}, max stack size: {}.",
            indexHeader.getCreateTime(),
            indexHeader.getSize(),
            capacity,
            indexHeader.getMaxCollision()
        );
    }

    public static IndexReader init(File sourceFile, File indexFile) throws IOException {
        return init(DEFAULT_CHANNELS, sourceFile, indexFile);
    }

    public static IndexReader init(int channels, File sourceFile, File indexFile) throws IOException {
        return new IndexReader(channels, sourceFile, indexFile);
    }

    public void cleanup() throws IOException {
        for (FileChannel sourceFileChannel : sourceFileChannels) {
            if (sourceFileChannel != null) {
                sourceFileChannel.close();
            }
        }
        for (FileChannel indexFileChannel : indexFileChannels) {
            if (indexFileChannel != null) {
                indexFileChannel.close();
            }
        }
        capacity = 0;
    }

    public <T> T loadByKey(String key, Function<byte[], T> unmarshaller, Function<T, String> keyExtractor) throws IOException {
        int hash = key.hashCode();
        int index = getIndex(hash, capacity);
        FileChannel indexFileChannel = getRandomIndexFileChannel();
        IndexNode indexNode = IndexNode.fromByteBuffer(
            loadByteBuffer(
                indexFileChannel,
                getNodeOffset(index),
                IndexNode.SIZE
            )
        );
        int size = indexNode.getCollision();
        if (size > 0) {
            ByteBuffer indexTunnelsBlock = loadByteBuffer(
                indexFileChannel,
                getTunnelOffset(firstTunnelIndex, indexNode.getOffset()),
                IndexTunnel.SIZE * size
            );
            FileChannel sourceFileChannel = getRandomSourceFileChannel();
            for (int i = 0; i < size; i++) {
                IndexTunnel indexTunnel = IndexTunnel.fromByteBuffer(indexTunnelsBlock, i);
                int actual = indexTunnel.getHash();
                if (actual == hash) {
                    byte[] data = loadByteBuffer(sourceFileChannel, indexTunnel.getOffset(), indexTunnel.getLength()).array();
                    T item = unmarshaller.apply(data);
                    if (key.equals(keyExtractor.apply(item))) {
                        return item;
                    }
                } else if (actual > hash) {
                    break;
                }
            }
        }
        return null;
    }

    public <T> List<T> loadByKeys(List<String> keys, Function<byte[], T> unmarshaller, Function<T, String> keyExtractor) throws IOException {
        List<T> items = new ArrayList<>();
        for (String key : keys) {
            T item = loadByKey(key, unmarshaller, keyExtractor);
            if (item != null) {
                items.add(item);
            }
        }
        return items;
    }


    private FileChannel getRandomSourceFileChannel() {
        return sourceFileChannels[nextInt(0, sourceFileChannels.length)];
    }

    private FileChannel getRandomIndexFileChannel() {
        return indexFileChannels[nextInt(0, indexFileChannels.length)];
    }

}

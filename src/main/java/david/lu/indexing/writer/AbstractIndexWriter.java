package david.lu.indexing.writer;

import david.lu.indexing.pojo.IndexNode;
import david.lu.indexing.pojo.IndexTunnel;
import io.vavr.Tuple2;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static david.lu.indexing.utils.IndexUtils.appendTunnel;
import static david.lu.indexing.utils.IndexUtils.byteToByteBuffer;
import static david.lu.indexing.utils.IndexUtils.formatTime;
import static david.lu.indexing.utils.IndexUtils.formatToMillions;
import static david.lu.indexing.utils.IndexUtils.getCapacity;
import static david.lu.indexing.utils.IndexUtils.getFirstTunnelOffset;
import static david.lu.indexing.utils.IndexUtils.getNodeOffset;
import static david.lu.indexing.utils.IndexUtils.getTunnelOffset;
import static david.lu.indexing.utils.IndexUtils.intToByte;
import static david.lu.indexing.utils.IndexUtils.intToByteBuffer;
import static david.lu.indexing.utils.IndexUtils.longToByteBuffer;
import static java.util.Collections.sort;
import static java.util.Comparator.comparing;

@Slf4j
public abstract class AbstractIndexWriter {
    private final static int BLOCK = 4096;
    private final int capacity;
    private final FileChannel indexFileChannel;
    protected final InputStream sourceInput;
    protected long offset;

    /**
     * @param size Estimate record count
     * @param sourceFilePath
     * @param indexFilePath
     * @throws IOException
     */
    AbstractIndexWriter(int size, String sourceFilePath, String indexFilePath) throws IOException {
        File indexFile = new File(indexFilePath);
        indexFile.getParentFile().mkdirs();
        this.capacity = getCapacity(size);
        this.sourceInput = new BufferedInputStream(new FileInputStream(sourceFilePath), BLOCK);
        this.indexFileChannel = new RandomAccessFile(indexFilePath, "rw").getChannel();
    }

    public void process() throws IOException {
        log.debug("==================== Starting index process... ====================");
        long indexStartTime = System.currentTimeMillis();
        log.debug("Index started @ {}", indexStartTime);
        // processed items
        long processed = 0l;
        // max collision
        int maxCollision = 0;
        List<IndexTunnel>[] tunnelLists = new ArrayList[capacity];
        IndexTunnel[] itemIndexes = nextItemIndexes();
        while (itemIndexes != null) {
            processed ++;
            if (processed % 0x10000 == 0) {
                System.gc();
                log.debug(
                    "{} items indexed.",
                    processed
                );
                log.debug(
                    "Memory - max: {} M, total: {} M, free: {} M",
                    formatToMillions(Runtime.getRuntime().maxMemory()),
                    formatToMillions(Runtime.getRuntime().totalMemory()),
                    formatToMillions(Runtime.getRuntime().freeMemory())
                );
            }
            for (IndexTunnel itemIndex : itemIndexes) {
                maxCollision = Math.max(maxCollision, appendTunnel(tunnelLists, itemIndex, capacity));
            }
            itemIndexes = nextItemIndexes();
        }
        long indexCompleteTime = System.currentTimeMillis();
        log.debug(
            "Index completed @ {}, time cost is {}",
            indexCompleteTime,
            formatTime(indexCompleteTime - indexStartTime)
        );
        log.debug("Node capacity: {}", capacity);
        log.debug("{} items processed.", processed);
        log.debug("Max stack size: {}", maxCollision);
        log.debug("==================== Index process finished ====================\n");

        log.debug("==================== Starting generating index file... ====================");
        // indexed items
        int indexed = 0;
        long firstTunnelIndex = getFirstTunnelOffset(capacity);

        long dumpStartTime = System.currentTimeMillis();
        indexFileChannel.write(longToByteBuffer(processed), 8);
        indexFileChannel.write(intToByteBuffer(capacity), 16);
        indexFileChannel.write(byteToByteBuffer(intToByte(maxCollision)), 20);
        for (int i = 0; i< tunnelLists.length; i++) {
            List<IndexTunnel> tunnels = tunnelLists[i];
            if (tunnels != null) {
                sort(tunnels, comparing(IndexTunnel::getHash));
                int collision = tunnels.size();
                IndexNode node = IndexNode.builder()
                    .offset(indexed)
                    .collision(intToByte(collision))
                    .build();
                indexFileChannel.write(node.toByteBuffer(), getNodeOffset(i));

                for (IndexTunnel tunnel : tunnels) {
                    indexFileChannel.write(tunnel.toByteBuffer(), getTunnelOffset(firstTunnelIndex, indexed));
                    indexed++;
                }
            }
        }
        long dumpCompleteTime = System.currentTimeMillis();
        indexFileChannel.write(longToByteBuffer(dumpCompleteTime), 0);
        log.debug("Indexing completed time: {}", dumpCompleteTime);
        log.debug(
                "{} items indexed in {}.",
                processed,
                formatTime(dumpCompleteTime - dumpStartTime)
        );
        indexFileChannel.close();
        log.debug("==================== Indexing file created ====================\n");

    }

    private IndexTunnel[] nextItemIndexes() throws IOException {
        Tuple2<Long, byte[]> item = nextItem();
        if (item == null) {
            return null;
        }
        long start = item._1();
        byte[] data = item._2();
        int length = data.length;
        int[] hashes = hash(data);
        IndexTunnel[] indexTunnels = new IndexTunnel[hashes.length];
        for (int i = 0; i < hashes.length; i++) {
            indexTunnels[i] = IndexTunnel.builder()
                .offset(start)
                .length(length)
                .hash(hashes[i])
                .build();
        }
        return indexTunnels;
    }

    protected abstract Tuple2<Long, byte[]> nextItem() throws IOException;

    protected abstract int[] hash(byte[] data) throws IOException;
}

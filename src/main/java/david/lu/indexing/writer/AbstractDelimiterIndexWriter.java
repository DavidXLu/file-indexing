package david.lu.indexing.writer;

import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractDelimiterIndexWriter extends AbstractIndexWriter {

    private final Set<Integer> delimiters;

    protected AbstractDelimiterIndexWriter(int size, String sourceFilePath, String indexFilePath, char[] delimiters) throws IOException {
        super(size, sourceFilePath, indexFilePath);
        this.delimiters = new HashSet<>();
        for (char delimiter : delimiters) {
            this.delimiters.add((int) delimiter);
        }
    }

    @Override
    protected Tuple2<Long, byte[]> nextItem() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int bt;
        long start = offset;
        while ((bt = sourceInput.read()) != -1) {
            offset++;
            if (delimiters.contains(bt)) {
                if (buffer.size() == 0) {
                    // skip empty item
                    start = offset;
                } else {
                    break;
                }
            } else {
                buffer.write(bt);
            }
        }
        return buffer.size() == 0 ? null : Tuple.of(start, buffer.toByteArray());
    }
}

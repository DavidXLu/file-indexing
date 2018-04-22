package david.lu.indexing.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import david.lu.indexing.writer.AbstractDelimiterIndexWriter;

import java.io.IOException;

public class DelimiterIndexWriter extends AbstractDelimiterIndexWriter {

    public DelimiterIndexWriter(int size, String sourceFilePath, String indexFilePath, char[] delimiters) throws IOException {
        super(size, sourceFilePath, indexFilePath, delimiters);
    }

    @Override
    protected int[] hash(byte[] data) throws IOException {
        Data obj = new ObjectMapper().readValue(data, Data.class);
        return new int[] {
            obj.getId().hashCode(),
            obj.getAlias().hashCode()
        };
    }
}

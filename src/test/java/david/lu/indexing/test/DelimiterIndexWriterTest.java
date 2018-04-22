package david.lu.indexing.test;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static david.lu.indexing.test.TestUtils.BIG_SIZE;
import static david.lu.indexing.test.TestUtils.INDEX_FILE_RESOURCE_LARGE;
import static david.lu.indexing.test.TestUtils.INDEX_FILE_RESOURCE_SAMPLE;
import static david.lu.indexing.test.TestUtils.SMALL_SIZE;
import static david.lu.indexing.test.TestUtils.SOURCE_FILE_RESOURCE_LARGE;
import static david.lu.indexing.test.TestUtils.SOURCE_FILE_RESOURCE_SAMPLE;

public class DelimiterIndexWriterTest {

    @Test
    public void smallDataTest() throws IOException {
        new DelimiterIndexWriter(
            SMALL_SIZE,
            SOURCE_FILE_RESOURCE_SAMPLE,
            INDEX_FILE_RESOURCE_SAMPLE,
            new char[] {'\r', '\n'}
        ).process();
    }

    // TODO ignored test
    @Ignore
    @Test
    public void largeDataTest() throws IOException {
        new DelimiterIndexWriter(
            BIG_SIZE,
            SOURCE_FILE_RESOURCE_LARGE,
            INDEX_FILE_RESOURCE_LARGE,
            new char[] {'\r', '\n'}
        ).process();
    }
}

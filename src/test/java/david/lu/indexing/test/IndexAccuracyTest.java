package david.lu.indexing.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import david.lu.indexing.reader.IndexReader;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static david.lu.indexing.test.TestUtils.INDEX_FILE_RESOURCE_LARGE;
import static david.lu.indexing.test.TestUtils.SOURCE_FILE_RESOURCE_LARGE;
import static org.assertj.core.api.Assertions.assertThat;

// TODO ignored test
@Slf4j
@Ignore
public class IndexAccuracyTest {
    private IndexReader reader;

    private ObjectMapper objectMapper;

    @Before
    public void setUp() throws IOException {
        objectMapper = new ObjectMapper();
        reader = IndexReader.init(
            new File(SOURCE_FILE_RESOURCE_LARGE),
            new File(INDEX_FILE_RESOURCE_LARGE)
        );
    }

    @Test
    public void test() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(SOURCE_FILE_RESOURCE_LARGE));
        String line;
        // processed items
        int processed = 0;
        while ((line = bufferedReader.readLine()) != null) {
            processed ++;
            Data expected = objectMapper.readValue(line, Data.class);
            String expectedId = expected.getId();
            assertThat(reader.loadByKey(expectedId, this::unmarshal, Data::getId).getId()).isEqualTo(expectedId);
            String expectedAlias = expected.getAlias();
            assertThat(reader.loadByKey(expectedAlias, this::unmarshal, Data::getAlias).getAlias()).isEqualTo(expectedAlias);
        }
        log.debug("{} items processed.", processed);
        bufferedReader.close();
    }

    @After
    public void tearDown() throws IOException {
        if (reader != null) {
            reader.cleanup();
        }
    }

    private Data unmarshal(byte[] bytes) {
        return Try.of(() -> objectMapper.readValue(bytes, Data.class))
            .onFailure(e -> log.error("Unmarshal failed.", e))
            .getOrNull();
    }

}

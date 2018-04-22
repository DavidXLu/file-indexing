package david.lu.indexing.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import david.lu.indexing.reader.IndexReader;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static david.lu.indexing.test.TestUtils.INDEX_FILE_RESOURCE_SAMPLE;
import static david.lu.indexing.test.TestUtils.SAMPLE_ALIAS;
import static david.lu.indexing.test.TestUtils.SAMPLE_IDS;
import static david.lu.indexing.test.TestUtils.SOURCE_FILE_RESOURCE_SAMPLE;
import static david.lu.indexing.utils.IndexUtils.DEFAULT_CAPACITY;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class IndexReaderTest {
    private IndexReader reader;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws IOException, URISyntaxException {
        File indexFile = new File(INDEX_FILE_RESOURCE_SAMPLE);
        if (!indexFile.exists()) {
            new DelimiterIndexWriter(
                DEFAULT_CAPACITY,
                SOURCE_FILE_RESOURCE_SAMPLE,
                INDEX_FILE_RESOURCE_SAMPLE,
                new char[]{'\r', '\n'}
            ).process();
        }
        reader = IndexReader.init(
            new File(SOURCE_FILE_RESOURCE_SAMPLE),
            new File(INDEX_FILE_RESOURCE_SAMPLE)
        );
    }

    @Test
    public void testIds() throws IOException {
        runTest(SAMPLE_IDS, Data::getId);
    }

    @Test
    public void testAlias() throws IOException {
        runTest(SAMPLE_ALIAS, Data::getAlias);
    }

    private void runTest(List<String> keys, Function<Data, String> keyExtractor) throws IOException {
        long s = System.currentTimeMillis();
        List<Data> items = reader.loadByKeys(keys, this::unmarshal, keyExtractor);
        assertThat(items).isNotEmpty();
        log.debug("Time cost: {}", System.currentTimeMillis() - s);
        List<String> actual = items.stream()
            .map(keyExtractor)
            .collect(Collectors.toList());
        assertThat(actual).containsExactlyElementsOf(keys);
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

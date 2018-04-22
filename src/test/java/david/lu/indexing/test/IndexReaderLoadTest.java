package david.lu.indexing.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import david.lu.indexing.reader.IndexReader;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static david.lu.indexing.test.TestUtils.INDEX_FILE_RESOURCE_LARGE;
import static david.lu.indexing.test.TestUtils.SOURCE_FILE_RESOURCE_LARGE;
import static david.lu.indexing.test.TestUtils.TEST_ALIAS_PATH;
import static david.lu.indexing.test.TestUtils.TEST_ID_PATH;
import static org.assertj.core.api.Assertions.assertThat;

// TODO ignored test
@Slf4j
@Ignore
public class IndexReaderLoadTest {
    // load repeats
    private static final int LOAD_REPEATS = 10000;
    // concurrency size
    private static final int CONCURRENCY = 50;

    // channels
    private static final int CHANNELS = 5;

    private static final int SIZE_MIN = 1;
    private static final int SIZE_MAX = 100;

    private ListeningExecutorService service;

    private Object lock;

    private ObjectMapper objectMapper;

    private int[] testIdCounts;
    private int[] testTimeCosts;

    private IndexReader reader;

    // load test time count
    private AtomicInteger times;

    @Before
    public void setUp() throws IOException {
        lock = new Object();
        objectMapper = new ObjectMapper();
        testIdCounts = new int[LOAD_REPEATS];
        testTimeCosts = new int[LOAD_REPEATS];
        times = new AtomicInteger();
        service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(CONCURRENCY));
        reader = IndexReader.init(
            CHANNELS,
            new File(SOURCE_FILE_RESOURCE_LARGE),
            new File(INDEX_FILE_RESOURCE_LARGE)
        );

    }

    @Test
    public void testIds() throws IOException, InterruptedException {
        TypeReference stringList = new TypeReference<List<String>>(){};
        List<String> ids = objectMapper.readValue(new FileInputStream(TEST_ID_PATH), stringList);
        Runnable taskDispatcher = new Runnable() {
            @Override
            public void run() {
                int index = times.getAndIncrement();
                if (index < LOAD_REPEATS) {
                    service.submit(
                        new Task(
                            index,
                            () -> TestUtils.randomSubList(ids, SIZE_MIN, SIZE_MAX),
                            Data::getId
                        )
                    ).addListener(this, service);
                }
            }
        };
        runTest(taskDispatcher);
    }

    @Test
    public void testAlias() throws IOException, InterruptedException {
        TypeReference stringList = new TypeReference<List<String>>(){};
        List<String> alias = objectMapper.readValue(new FileInputStream(TEST_ALIAS_PATH), stringList);
        Runnable taskDispatcher = new Runnable() {
            @Override
            public void run() {
                int index = times.getAndIncrement();
                if (index < LOAD_REPEATS) {
                    service.submit(
                        new Task(
                            index,
                            () -> TestUtils.randomSubList(alias, SIZE_MIN, SIZE_MAX),
                            Data::getAlias
                        )
                    ).addListener(this, service);
                }
            }
        };
        runTest(taskDispatcher);
    }

    private void runTest(Runnable taskDispatcher) throws IOException, InterruptedException {
        for (int i = 0; i < CONCURRENCY; i++) {
            taskDispatcher.run();
        }
        synchronized (lock) {
            lock.wait();
        }
        log.debug("Concurrency: {}, repeat {} times.", CONCURRENCY, LOAD_REPEATS);
        printTimeCosts();
    }

    private void printTimeCosts() {
        int minTimeCost = Integer.MAX_VALUE;
        int maxTimeCost = Integer.MIN_VALUE;
        int totalTimeCost = 0;
        int minTestIdCount = SIZE_MAX;
        int maxTestIdCount = SIZE_MIN;
        int totalTestIdCount = 0;
        for (int i = 0; i < LOAD_REPEATS; i++) {
            int testTimeCost = testTimeCosts[i];
            totalTimeCost += testTimeCost;
            minTimeCost = Math.min(minTimeCost, testTimeCost);
            maxTimeCost = Math.max(maxTimeCost, testTimeCost);
            int testIdCount = testIdCounts[i];
            totalTestIdCount += testIdCount;
            minTestIdCount = Math.min(minTestIdCount, testIdCount);
            maxTestIdCount = Math.max(maxTestIdCount, testIdCount);
        }
        log.debug("Test item counts: average = {}, min = {}, max = {}", totalTestIdCount / LOAD_REPEATS, minTestIdCount, maxTestIdCount);
        log.debug("Time cost: average = {} ms, min = {} ms, max = {} ms", totalTimeCost / LOAD_REPEATS, minTimeCost, maxTimeCost);
    }

    @After
    public void tearDown() throws IOException {
        reader.cleanup();
    }

    class Task implements Runnable {
        private int index;
        private Supplier<List<String>> keySupplier;
        private Function<Data, String> keyExtractor;

        public Task(int index, Supplier<List<String>> keySupplier, Function<Data, String> keyExtractor) {
            this.index = index;
            this.keySupplier = keySupplier;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void run() {
            try {
                List<String> keys = keySupplier.get();
                testIdCounts[index] = keys.size();
                long s = System.currentTimeMillis();
                List<Data> items = reader.loadByKeys(keys, bytes -> unmarshal(bytes), keyExtractor);
                int timeCost = (int) (System.currentTimeMillis() - s);
                testTimeCosts[index] = timeCost;
                List<String> actual = items.stream()
                    .map(keyExtractor)
                    .collect(Collectors.toList());
                assertThat(actual).containsExactlyElementsOf(keys);
            } catch (IOException e) {
                Fail.fail(String.format("[Task#%d]Shouldn't throw exception", index), e);
            } finally {
                if (index + 1 >= LOAD_REPEATS) {
                    log.debug("Task finished");
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        }
    }

    private Data unmarshal(byte[] bytes) {
        return Try.of(() -> objectMapper.readValue(bytes, Data.class))
            .onFailure(e -> log.error("Unmarshal failed.", e))
            .getOrNull();
    }
}

package david.lu.indexing.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static david.lu.indexing.utils.IndexUtils.DEFAULT_CAPACITY;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.RandomUtils.nextInt;

public class TestUtils {

    public static final int SMALL_SIZE = DEFAULT_CAPACITY;
    public static final int BIG_SIZE = 0x1ffffff;
    public static final String SOURCE_FILE_RESOURCE_SAMPLE = "src/test/data/sample.data";
    public static final String INDEX_FILE_RESOURCE_SAMPLE = "src/test/data/sample.index";
    // TODO update path to large data set
    public static final String SOURCE_FILE_RESOURCE_LARGE = "[large source file]";
    public static final String INDEX_FILE_RESOURCE_LARGE = "[large indexing file]";
    public static final String TEST_ID_PATH = "[large source ids]";
    public static final String TEST_ALIAS_PATH = "[large source aliases]";

    public static final List<String> SAMPLE_IDS = asList(
        "--1-O93k_1M7vlfoje1uyg",
        "--2hyZC3-y6BppkSghvOFw",
        "--82WCIJKzkyMUIDT9QK5Q",
        "--G2FEGIxApWg62fRyLgjg",
        "--KGwXYMJoLK9v3I1TviIA",
        "--KTliY7t2sd4atRWdJtVw",
        "--LUapetRSkZpFZ2d-MXLQ",
        "-05ARBpak3_KTPu3YSo2ZA",
        "-08tPj7h6CmxsidEWPF6kg",
        "-06d0pz-C8gVoTDojnk9uQ",
        "-5ucArGIrKBrf4x8WBwniA"
    );

    public static final List<String> SAMPLE_ALIAS = asList(
        "crazy-horse-cabaret-pleasantville",
        "diganet-denver",
        "artzs-liquor-and-deli-taft",
        "re-surface-manassas",
        "artificial-turf-new-hyde-park",
        "marathon-gas-station-miami-7",
        "byrne-moore-and-davis-atlanta",
        "avenue-southfield",
        "mckinney-jewelry-and-loan-mc-kinney",
        "martin-j-and-sons-cleaners-worth",
        "atlas-motel-cheyenne"
    );

    public static <T> List<T> randomSubList(List<T> list, int min, int max) {
        int size = nextInt(min, max);
        List<T> copy = new ArrayList<>(list);
        Collections.shuffle(copy);
        return copy.subList(0, size);
    }

}

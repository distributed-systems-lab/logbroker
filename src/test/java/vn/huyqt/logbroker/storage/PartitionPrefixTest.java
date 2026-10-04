package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

class PartitionPrefixTest {
    @TempDir Path root;

    @Test
    void deletesOnlyWholeSealedSegmentsAndReopensFromRetainedStart() throws Exception {
        var config = new LogConfig(128, 128, 32);
        var records = List.of(new LogRecord(0, null, new byte[40], List.of()));
        long retained;
        try (var log = PartitionLog.open(root, config)) {
            for (int i = 0; i < 5; i++) log.append(records);
            assertEquals(2, log.prefixStartAfter(2));
            retained = log.deleteSegmentsBefore(2);
            assertEquals(2, retained);
            assertEquals(2, log.logStartOffset());
            assertEquals(2, log.read(2, 1).getFirst().baseOffset());
            assertEquals(4, log.deleteSegmentsBefore(100)); // active segment remains
        }
        try (var log =
                PartitionLog.open(root, config, new LogOpenOptions(4, 5, false, path -> {}))) {
            assertEquals(4, log.logStartOffset());
            assertEquals(5, log.logEndOffset());
        }
    }
}

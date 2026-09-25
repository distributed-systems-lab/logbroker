package vn.huyqt.logbroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class PartitionTruncateTest {
    @Test void truncatesAtBatchBoundaryAndReusesTailOffsets(@TempDir Path dir) throws Exception {
        try (var log = PartitionLog.open(dir, new LogConfig(256, 128, 64))) {
            log.append(StorageFixtures.records("a", "b"));
            log.append(StorageFixtures.records("c"));
            assertThrows(IllegalArgumentException.class, () -> log.truncateTo(1));
            assertEquals(3, log.logEndOffset());
            log.truncateTo(2);
            assertEquals(2, log.logEndOffset());
            assertEquals(2, log.durableEndOffset());
            assertEquals(new AppendResult(2, 3), log.append(StorageFixtures.records("replacement")));
        }
    }

    @Test void truncatesAcrossSegmentsAndKeepsBoundarySegment(@TempDir Path dir) throws Exception {
        var config = new LogConfig(128, 128, 64);
        try (var log = PartitionLog.open(dir, config)) {
            for (int i = 0; i < 5; i++) log.append(StorageFixtures.records("a"));
            log.truncateTo(2);
            assertEquals(2, log.logEndOffset());
            assertEquals(2, log.read(0, 1024).size());
            assertTrue(Files.exists(LogSegment.dataPath(dir, 2)));
            assertEquals(0, Files.size(LogSegment.dataPath(dir, 2)));
            assertFalse(Files.exists(LogSegment.dataPath(dir, 4)));
            assertEquals(new AppendResult(2, 3), log.append(StorageFixtures.records("b")));
        }
        try (var reopened = PartitionLog.open(dir, config)) {
            assertEquals(3, reopened.logEndOffset());
        }
    }

    @Test void supportsZeroEndAndRejectsOutsideRange(@TempDir Path dir) throws Exception {
        try (var log = PartitionLog.open(dir, new LogConfig(4096, 1024, 64))) {
            log.append(StorageFixtures.records("a"));
            log.truncateTo(1);
            assertThrows(IllegalArgumentException.class, () -> log.truncateTo(-1));
            assertThrows(IllegalArgumentException.class, () -> log.truncateTo(2));
            log.truncateTo(0);
            assertEquals(0, log.logEndOffset());
            assertEquals(new AppendResult(0, 1), log.append(StorageFixtures.records("b")));
        }
    }

    @Test void failedDeleteCanRecoverAndRetry(@TempDir Path dir) throws Exception {
        var config = new LogConfig(128, 128, 64);
        var io = new ScriptedLogIo();
        var log = PartitionLog.open(dir, config, io);
        for (int i = 0; i < 5; i++) log.append(StorageFixtures.records("a"));
        io.failDeleteAt = 2;
        assertThrows(IOException.class, () -> log.truncateTo(2));
        assertThrows(IllegalStateException.class, log::logEndOffset);
        log.close();
        try (var reopened = PartitionLog.open(dir, config)) {
            reopened.truncateTo(2);
            assertEquals(2, reopened.logEndOffset());
        }
    }
}

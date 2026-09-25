package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class PartitionLogTest {
    @Test
    void appendReadFlushAndReopen(@TempDir Path dir) throws Exception {
        var config = new LogConfig(4096, 1024, 64);
        try (var log = PartitionLog.open(dir, config)) {
            assertEquals(new AppendResult(0, 2), log.append(StorageFixtures.records("a", "b")));
            assertEquals(0, log.durableEndOffset());
            var batch = log.read(1, 1).getFirst();
            assertEquals(0, batch.baseOffset());
            assertEquals(2, batch.nextOffset());
            assertTrue(log.read(2, 100).isEmpty());
            assertEquals(2, log.flush());
        }
        try (var log = PartitionLog.open(dir, config)) {
            assertEquals(2, log.logEndOffset());
            assertEquals(2, log.durableEndOffset());
            assertEquals(StorageFixtures.records("a", "b"), log.read(0, 1024).getFirst().records());
            assertEquals(new AppendResult(2, 3), log.append(StorageFixtures.records("c")));
        }
    }

    @Test
    void rollsAtBatchBoundaryAndReadsWithinBudget(@TempDir Path dir) throws Exception {
        var config = new LogConfig(128, 128, 64);
        try (var log = PartitionLog.open(dir, config)) {
            log.append(StorageFixtures.records("a"));
            log.append(StorageFixtures.records("b"));
            log.append(StorageFixtures.records("c"));
            assertEquals(2, log.durableEndOffset());
            assertEquals(2, log.read(0, 102).size());
            assertEquals(1, log.read(0, 1).size());
            assertEquals(3, log.read(1, 1024).getLast().nextOffset());
            assertEquals(51, Files.size(LogSegment.dataPath(dir, 2)));
            assertThrows(IllegalArgumentException.class, () -> log.read(4, 10));
            assertThrows(IllegalArgumentException.class, () -> log.read(0, 0));
        }
    }

    @Test
    void validationDoesNotConsumeOffsetsAndLockIsExclusive(@TempDir Path dir) throws Exception {
        var config = new LogConfig(4096, 1024, 64);
        try (var log = PartitionLog.open(dir, config)) {
            assertThrows(IllegalArgumentException.class, () -> log.append(java.util.List.of()));
            assertEquals(0, log.logEndOffset());
            assertThrows(IOException.class, () -> PartitionLog.open(dir, config));
            assertEquals(new AppendResult(0, 1), log.append(StorageFixtures.records("a")));
            var saved = log.read(0, 1024);
            assertEquals(1, saved.getFirst().nextOffset());
        }
        assertTrue(Files.exists(dir.resolve(".lock")));
        try (var reopened = PartitionLog.open(dir, config)) {
            assertEquals(1, reopened.logEndOffset());
        }
    }
}

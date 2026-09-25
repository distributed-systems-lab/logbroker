package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

class PartitionDurabilityTest {
    private static final LogConfig CONFIG = new LogConfig(4096, 1024, 64);

    @Test
    void failedFlushPoisonsLogButReleasesLockOnClose(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        var log = PartitionLog.open(dir, CONFIG, io);
        log.append(StorageFixtures.records("a"));
        assertEquals(0, log.durableEndOffset());
        io.failForce = true;
        assertThrows(IOException.class, log::flush);
        assertThrows(IllegalStateException.class, () -> log.read(0, 1024));
        assertThrows(IllegalStateException.class, () -> log.append(StorageFixtures.records("b")));
        int attempts = io.forceCalls;
        log.close();
        assertEquals(attempts, io.forceCalls);
        try (var reopened = PartitionLog.open(dir, CONFIG)) {
            assertEquals(1, reopened.logEndOffset());
        }
    }

    @Test
    void appendDoesNotForceAndFlushGroupsWrites(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        try (var log = PartitionLog.open(dir, CONFIG, io)) {
            io.forceCalls = 0;
            log.append(StorageFixtures.records("a"));
            log.append(StorageFixtures.records("b"));
            assertEquals(0, io.forceCalls);
            assertEquals(0, log.durableEndOffset());
            assertEquals(2, log.flush());
            assertEquals(1, io.forceCalls);
        }
    }

    @Test
    void partialWriteFailsAndRecoveryDropsTail(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        var log = PartitionLog.open(dir, CONFIG, io);
        log.append(StorageFixtures.records("a"));
        log.flush();
        io.failAfterBytes = io.writtenBytes + 10;
        IOException first =
                assertThrows(IOException.class, () -> log.append(StorageFixtures.records("b")));
        IllegalStateException later = assertThrows(IllegalStateException.class, log::flush);
        assertSame(first, later.getCause());
        log.close();
        try (var reopened = PartitionLog.open(dir, CONFIG)) {
            assertEquals(1, reopened.logEndOffset());
        }
    }

    @Test
    void indexFailureMayLeaveCompleteBatchForRecovery(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        var log = PartitionLog.open(dir, CONFIG, io);
        io.failAfterBytes = io.writtenBytes + 51;
        assertThrows(IOException.class, () -> log.append(StorageFixtures.records("a")));
        assertThrows(IllegalStateException.class, log::logEndOffset);
        log.close();
        try (var reopened = PartitionLog.open(dir, CONFIG)) {
            assertEquals(1, reopened.logEndOffset());
        }
    }

    @Test
    void closeForceFailureStillReleasesLock(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        var log = PartitionLog.open(dir, CONFIG, io);
        log.append(StorageFixtures.records("a"));
        io.failForce = true;
        assertThrows(IOException.class, log::close);
        log.close();
        try (var reopened = PartitionLog.open(dir, CONFIG)) {
            assertEquals(1, reopened.logEndOffset());
        }
    }

    @Test
    void openFailureReleasesLockAndValidationDoesNotPoison(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        io.failForce = true;
        assertThrows(IOException.class, () -> PartitionLog.open(dir, CONFIG, io));
        try (var log = PartitionLog.open(dir, CONFIG)) {
            assertThrows(IllegalArgumentException.class, () -> log.append(java.util.List.of()));
            assertEquals(new AppendResult(0, 1), log.append(StorageFixtures.records("a")));
        }
    }
}

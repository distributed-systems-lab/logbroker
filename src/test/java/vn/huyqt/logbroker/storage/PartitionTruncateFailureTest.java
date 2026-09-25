package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

class PartitionTruncateFailureTest {
    @Test
    void truncateFailureAfterSuffixDeleteCanRecoverAndRetry(@TempDir Path dir) throws Exception {
        var config = new LogConfig(128, 128, 64);
        var io = new ScriptedLogIo();
        var log = PartitionLog.open(dir, config, io);
        for (int i = 0; i < 5; i++) log.append(StorageFixtures.records("a"));
        io.failTruncate = true;
        assertThrows(IOException.class, () -> log.truncateTo(2));
        assertThrows(IllegalStateException.class, log::logEndOffset);
        log.close();
        try (var reopened = PartitionLog.open(dir, config)) {
            assertEquals(4, reopened.logEndOffset());
            reopened.truncateTo(2);
            assertEquals(2, reopened.logEndOffset());
        }
    }
}

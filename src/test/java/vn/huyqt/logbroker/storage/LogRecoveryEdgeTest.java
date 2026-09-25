package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

class LogRecoveryEdgeTest {
    private static final LogConfig CONFIG = new LogConfig(4096, 1024, 64);

    @Test
    void rejectsEmptySealedSegment(@TempDir Path dir) throws Exception {
        Files.write(LogSegment.dataPath(dir, 0), new byte[0]);
        byte[] second = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        StorageFixtures.writeBatch(dir, 1, second);
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(dir, CONFIG, new LogIo()));
        assertEquals(0, Files.size(LogSegment.dataPath(dir, 0)));
        assertArrayEquals(second, Files.readAllBytes(LogSegment.dataPath(dir, 1)));
    }

    @Test
    void rejectsOffsetGapAndDuplicateWithoutChangingData(@TempDir Path root) throws Exception {
        for (long offset : new long[] {0, 2}) {
            Path dir = Files.createDirectory(root.resolve("offset" + offset));
            StorageFixtures.writeBatch(
                    dir, 0, BatchCodec.encode(0, StorageFixtures.records("a"), 1024));
            StorageFixtures.writeBatch(
                    dir, 0, BatchCodec.encode(offset, StorageFixtures.records("b"), 1024));
            byte[] before = Files.readAllBytes(LogSegment.dataPath(dir, 0));
            assertThrows(
                    CorruptLogException.class, () -> LogRecovery.recover(dir, CONFIG, new LogIo()));
            assertArrayEquals(before, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
        }
    }

    @Test
    void invalidVersionAndOversizePartialHeaderAreNotTornTails(@TempDir Path root)
            throws Exception {
        byte[] valid = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        Path version = Files.createDirectory(root.resolve("version"));
        byte[] badVersion = Arrays.copyOf(valid, 6);
        badVersion[5] = 2;
        StorageFixtures.writeBatch(version, 0, badVersion);
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(version, CONFIG, new LogIo()));
        assertArrayEquals(badVersion, Files.readAllBytes(LogSegment.dataPath(version, 0)));

        Path length = Files.createDirectory(root.resolve("length"));
        byte[] badLength = Arrays.copyOf(valid, 10);
        ByteBuffer.wrap(badLength).order(ByteOrder.BIG_ENDIAN).putInt(6, 1025);
        StorageFixtures.writeBatch(length, 0, badLength);
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(length, CONFIG, new LogIo()));
        assertArrayEquals(badLength, Files.readAllBytes(LogSegment.dataPath(length, 0)));
    }

    @Test
    void missingOrTruncatedIndexIsRebuilt(@TempDir Path dir) throws Exception {
        StorageFixtures.writeBatch(
                dir, 0, BatchCodec.encode(0, StorageFixtures.records("a"), 1024));
        Path index = OffsetIndex.indexPath(dir, 0);
        assertFalse(Files.exists(index));
        LogRecovery.recover(dir, CONFIG, new LogIo());
        assertEquals(16, Files.size(index));
        Files.write(index, new byte[] {1, 2, 3});
        LogRecovery.recover(dir, CONFIG, new LogIo());
        assertEquals(16, Files.size(index));
    }

    @Test
    void forceFailureDoesNotReturnRecoveredResult(@TempDir Path dir) throws Exception {
        byte[] data = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        StorageFixtures.writeBatch(dir, 0, data);
        LogIo io =
                new LogIo() {
                    @Override
                    void force(FileChannel channel) throws IOException {
                        throw new IOException("Injected force failure");
                    }
                };
        assertThrows(IOException.class, () -> LogRecovery.recover(dir, CONFIG, io));
        assertArrayEquals(data, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
    }

    @Test
    void smallerReopenLimitRejectsExistingBatch(@TempDir Path dir) throws Exception {
        byte[] data = BatchCodec.encode(0, StorageFixtures.records("large payload"), 1024);
        StorageFixtures.writeBatch(dir, 0, data);
        assertThrows(
                CorruptLogException.class,
                () -> LogRecovery.recover(dir, new LogConfig(4096, 50, 64), new LogIo()));
        assertArrayEquals(data, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
    }
}

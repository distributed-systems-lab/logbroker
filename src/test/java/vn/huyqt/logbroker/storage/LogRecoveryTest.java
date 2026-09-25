package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.CRC32C;

class LogRecoveryTest {
    private static final LogConfig CONFIG = new LogConfig(4096, 1024, 64);

    @Test
    void truncatesOnlyIncompleteActiveTail(@TempDir Path dir) throws Exception {
        byte[] a = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        byte[] b = BatchCodec.encode(1, StorageFixtures.records("b"), 1024);
        StorageFixtures.writeBatch(dir, 0, a);
        StorageFixtures.writeBatch(dir, 0, Arrays.copyOf(b, b.length - 1));
        var result = LogRecovery.recover(dir, CONFIG, new LogIo());
        assertEquals(1, result.nextOffset());
        assertEquals(a.length, Files.size(LogSegment.dataPath(dir, 0)));
    }

    @Test
    void rejectsCompleteBadChecksumWithoutMutation(@TempDir Path dir) throws Exception {
        byte[] bytes = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        bytes[bytes.length - 1] ^= 1;
        StorageFixtures.writeBatch(dir, 0, bytes);
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(dir, CONFIG, new LogIo()));
        assertArrayEquals(bytes, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
    }

    @Test
    void cutsEveryIncompleteFinalBatchPrefix(@TempDir Path root) throws Exception {
        byte[] a = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        byte[] b = BatchCodec.encode(1, StorageFixtures.records("b"), 1024);
        for (int cut = 1; cut < b.length; cut++) {
            Path dir = Files.createDirectory(root.resolve("cut" + cut));
            StorageFixtures.writeBatch(dir, 0, a);
            StorageFixtures.writeBatch(dir, 0, Arrays.copyOf(b, cut));
            assertEquals(1, LogRecovery.recover(dir, CONFIG, new LogIo()).nextOffset());
            assertArrayEquals(a, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
        }
    }

    @Test
    void rejectsOldSegmentTailWithoutMutation(@TempDir Path dir) throws Exception {
        byte[] a = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        byte[] b = BatchCodec.encode(1, StorageFixtures.records("b"), 1024);
        StorageFixtures.writeBatch(dir, 0, a);
        StorageFixtures.writeBatch(dir, 0, Arrays.copyOf(b, 4));
        StorageFixtures.writeBatch(dir, 1, b);
        byte[] before = Files.readAllBytes(LogSegment.dataPath(dir, 0));
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(dir, CONFIG, new LogIo()));
        assertArrayEquals(before, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
    }

    @Test
    void rejectsBadNamesGapsAndPlausibleButWrongHeader(@TempDir Path root) throws Exception {
        Path badName = Files.createDirectory(root.resolve("name"));
        Files.write(badName.resolve("bad.log"), new byte[0]);
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(badName, CONFIG, new LogIo()));

        Path gap = Files.createDirectory(root.resolve("gap"));
        StorageFixtures.writeBatch(
                gap, 1, BatchCodec.encode(1, StorageFixtures.records("a"), 1024));
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(gap, CONFIG, new LogIo()));

        Path wrong = Files.createDirectory(root.resolve("wrong"));
        byte[] bytes = BatchCodec.encode(1, StorageFixtures.records("a"), 1024);
        StorageFixtures.writeBatch(wrong, 0, Arrays.copyOf(bytes, 18));
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(wrong, CONFIG, new LogIo()));
    }

    @Test
    void rebuildsIndexAndAcceptsEmptyActive(@TempDir Path dir) throws Exception {
        byte[] a = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        StorageFixtures.writeBatch(dir, 0, a);
        Files.write(OffsetIndex.indexPath(dir, 0), new byte[] {1, 2, 3});
        Files.write(LogSegment.dataPath(dir, 1), new byte[0]);
        var result = LogRecovery.recover(dir, CONFIG, new LogIo());
        assertEquals(1, result.nextOffset());
        assertEquals(16, Files.size(OffsetIndex.indexPath(dir, 0)));
        assertEquals(2, result.segments().size());
    }

    @Test
    void rejectsMalformedPayloadWithCorrectCrc(@TempDir Path dir) throws Exception {
        byte[] bytes = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(38, -2);
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 26);
        crc.update(bytes, 30, bytes.length - 30);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(26, (int) crc.getValue());
        StorageFixtures.writeBatch(dir, 0, bytes);
        assertThrows(
                CorruptLogException.class, () -> LogRecovery.recover(dir, CONFIG, new LogIo()));
        assertArrayEquals(bytes, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
    }
}

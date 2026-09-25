package vn.huyqt.logbroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class LogSegmentTest {
    @Test void completesShortWrites(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        io.maxWriteBytes = 3;
        byte[] bytes = BatchCodec.encode(0, StorageFixtures.records("abc"), 1024);
        try (var segment = LogSegment.open(LogSegment.dataPath(dir, 0), 0, io)) {
            assertEquals(0, segment.append(bytes));
            assertArrayEquals(bytes, segment.readBytes(0, bytes.length));
            assertEquals(0, io.forceCalls);
            assertEquals(bytes.length, segment.size());
            assertThrows(EOFException.class, () -> segment.readBytes(1, bytes.length));
            segment.force();
            assertEquals(1, io.forceCalls);
            segment.truncate(10);
            assertEquals(10, segment.size());
        }
    }

    @Test void stopsZeroProgress(@TempDir Path dir) throws Exception {
        LogIo io = new LogIo() {
            @Override int write(FileChannel ch, ByteBuffer src, long position) { return 0; }
        };
        try (var segment = LogSegment.open(LogSegment.dataPath(dir, 0), 0, io)) {
            assertThrows(IOException.class, () -> segment.append(new byte[]{1}));
        }
    }

    @Test void reportsPartialWriteAndKeepsWrittenPrefix(@TempDir Path dir) throws Exception {
        var io = new ScriptedLogIo();
        io.failAfterBytes = 4;
        try (var segment = LogSegment.open(LogSegment.dataPath(dir, 0), 0, io)) {
            assertThrows(IOException.class, () -> segment.append(new byte[]{1, 2, 3, 4, 5}));
            assertArrayEquals(new byte[]{1, 2, 3, 4}, segment.readBytes(0, 4));
        }
    }
}

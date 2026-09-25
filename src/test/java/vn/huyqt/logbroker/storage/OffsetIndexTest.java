package vn.huyqt.logbroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class OffsetIndexTest {
    @Test void indexesFirstBatchAndByteIntervals() {
        var index = new OffsetIndex(100);
        index.consider(0, 0);
        index.consider(2, 60);
        index.consider(4, 120);
        assertEquals(new OffsetIndex.Entry(0, 0), index.floor(3));
        assertEquals(new OffsetIndex.Entry(4, 120), index.floor(4));
        assertNull(index.floor(-1));
        assertEquals(2, index.entries().size());
        assertThrows(UnsupportedOperationException.class, () -> index.entries().clear());
        assertThrows(IllegalArgumentException.class, () -> index.consider(4, 150));
        assertThrows(IllegalArgumentException.class, () -> index.consider(5, 120));
    }

    @Test void writesBigEndianAndOverwritesGarbage(@TempDir Path dir) throws Exception {
        var index = new OffsetIndex(10);
        assertNull(index.floor(0));
        index.consider(3, 0);
        index.consider(5, 12);
        Path path = OffsetIndex.indexPath(dir, 3);
        Files.write(path, new byte[]{1, 2, 3});
        index.write(path, new LogIo());
        byte[] bytes = Files.readAllBytes(path);
        assertEquals(32, bytes.length);
        var b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        assertEquals(3, b.getLong());
        assertEquals(0, b.getLong());
        assertEquals(5, b.getLong());
        assertEquals(12, b.getLong());
    }
}

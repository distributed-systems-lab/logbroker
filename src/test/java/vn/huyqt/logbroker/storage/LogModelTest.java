package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class LogModelTest {
    @Test
    void protectsRecordBytesAndPreservesNull() {
        byte[] bytes = {1, 2};
        var record = new LogRecord(0, null, bytes, List.of(new RecordHeader("x", null)));
        bytes[0] = 9;
        record.value()[1] = 9;
        assertArrayEquals(new byte[] {1, 2}, record.value());
        assertNull(record.key());
        assertNull(record.headers().getFirst().value());
    }

    @Test
    void keepsHeaderOrderAndDefendsLists() {
        var headers =
                new ArrayList<>(
                        List.of(new RecordHeader("x", new byte[0]), new RecordHeader("x", null)));
        var record = new LogRecord(3, new byte[0], null, headers);
        headers.clear();
        assertEquals(2, record.headers().size());
        assertNotEquals(record.headers().get(0), record.headers().get(1));
        assertArrayEquals(new byte[0], record.key());
        assertNull(record.value());
        assertThrows(UnsupportedOperationException.class, () -> record.headers().clear());
    }

    @Test
    void comparesByteContentsAndValidatesBounds() {
        assertEquals(new RecordHeader("x", new byte[] {1}), new RecordHeader("x", new byte[] {1}));
        assertEquals(
                new LogRecord(0, null, new byte[] {1}, List.of()),
                new LogRecord(0, null, new byte[] {1}, List.of()));
        assertEquals(12, new RecordBatch(10, StorageFixtures.records("a", "b"), 50).nextOffset());
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecordBatch(Long.MAX_VALUE, StorageFixtures.records("a"), 50));
        assertThrows(IllegalArgumentException.class, () -> new RecordBatch(0, List.of(), 50));
        assertThrows(IllegalArgumentException.class, () -> new LogConfig(49, 50, 1));
        assertThrows(IllegalArgumentException.class, () -> new LogConfig(100, 49, 1));
        assertThrows(IllegalArgumentException.class, () -> new LogConfig(100, 50, 0));
    }
}

package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.zip.CRC32C;

class BatchCodecTest {
    @Test
    void roundTripAndRejectCorruption() throws Exception {
        var records = StorageFixtures.records("a", "b");
        byte[] bytes = BatchCodec.encode(7, records, 1024);
        var decoded = BatchCodec.decode(bytes, 1024);
        assertEquals(7, decoded.baseOffset());
        assertEquals(9, decoded.nextOffset());
        assertEquals(records, decoded.records());
        bytes[bytes.length - 1] ^= 1;
        assertThrows(CorruptLogException.class, () -> BatchCodec.decode(bytes, 1024));
    }

    @Test
    void goldenOneRecord() throws Exception {
        byte[] expected = new byte[51];
        var b = ByteBuffer.wrap(expected).order(ByteOrder.BIG_ENDIAN);
        b.put(new byte[] {'D', 'L', 'O', 'G'}).putShort((short) 1).putInt(51);
        b.putLong(0).putInt(1).putInt(0).putInt(0);
        b.putLong(0).putInt(-1).putInt(1).put((byte) 65).putInt(0);
        refreshCrc(expected);
        byte[] encoded =
                BatchCodec.encode(
                        0, List.of(new LogRecord(0, null, new byte[] {65}, List.of())), 1024);
        assertArrayEquals(expected, encoded);
        assertEquals(1, BatchCodec.decode(expected, 1024).nextOffset());
    }

    @Test
    void preservesNullEmptyAndOrderedHeaders() throws Exception {
        var record =
                new LogRecord(
                        -8,
                        new byte[0],
                        null,
                        List.of(new RecordHeader("é", null), new RecordHeader("é", new byte[0])));
        assertEquals(
                record,
                BatchCodec.decode(BatchCodec.encode(2, List.of(record), 1024), 1024)
                        .records()
                        .getFirst());
    }

    @Test
    void validatesHeaderPrefixesAtEveryLength() throws Exception {
        byte[] bytes = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        for (int n = 0; n <= 30; n++) {
            BatchCodec.validateHeaderPrefix(java.util.Arrays.copyOf(bytes, n), 1024);
        }
        bytes[0] = 'X';
        assertThrows(
                CorruptLogException.class,
                () -> BatchCodec.validateHeaderPrefix(new byte[] {bytes[0]}, 1024));
    }

    @Test
    void rejectsInvalidHeaderFields() {
        byte[] base = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        assertInvalid(base, 4, 2, 2); // version
        assertInvalid(base, 22, 4, 1); // attributes
        assertInvalid(base, 6, 4, 49); // too short
        assertInvalid(base, 18, 4, 0); // empty batch
        assertInvalid(base, 18, 4, 100); // count exceeds payload
        assertInvalid(base, 10, 8, Long.MAX_VALUE); // offset overflow
        byte[] trailing = java.util.Arrays.copyOf(base, base.length + 1);
        assertThrows(CorruptLogException.class, () -> BatchCodec.decode(trailing, 1024));
    }

    @Test
    void rejectsInvalidPayloadFields() {
        byte[] base = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
        assertInvalid(base, 38, 4, -2); // invalid key length
        assertInvalid(base, 38, 4, 1000); // oversized key
        assertInvalid(base, 42, 4, -2); // invalid value length
        assertInvalid(base, 47, 4, 100); // header count exceeds payload
        byte[] withHeader =
                BatchCodec.encode(
                        0,
                        List.of(new LogRecord(0, null, null, List.of(new RecordHeader("x", null)))),
                        1024);
        withHeader[54] = (byte) 0xff; // malformed UTF-8 header key
        refreshCrc(withHeader);
        assertThrows(CorruptLogException.class, () -> BatchCodec.decode(withHeader, 1024));
    }

    @Test
    void rejectsCallerOverflowAndOversize() {
        assertThrows(
                IllegalArgumentException.class,
                () -> BatchCodec.encode(Long.MAX_VALUE, StorageFixtures.records("a"), 1024));
        assertThrows(
                IllegalArgumentException.class,
                () -> BatchCodec.encode(0, StorageFixtures.records("a"), 50));
    }

    private static void assertInvalid(byte[] base, int at, int width, long value) {
        byte[] changed = base.clone();
        ByteBuffer b = ByteBuffer.wrap(changed).order(ByteOrder.BIG_ENDIAN);
        if (width == 2) b.putShort(at, (short) value);
        else if (width == 4) b.putInt(at, (int) value);
        else b.putLong(at, value);
        refreshCrc(changed);
        assertThrows(CorruptLogException.class, () -> BatchCodec.decode(changed, 1024));
    }

    private static void refreshCrc(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 26);
        crc.update(bytes, 30, bytes.length - 30);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(26, (int) crc.getValue());
    }
}

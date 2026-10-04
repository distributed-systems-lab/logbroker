package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

class RecordPayloadCodecTest {
    @Test
    void writesTheExistingRecordFormatWithoutABatchHeader() throws Exception {
        var record = new LogRecord(7, null, new byte[0], List.of());
        byte[] expected = HexFormat.of().parseHex("0000000000000007ffffffff0000000000000000");
        assertEquals(20, RecordPayloadCodec.encodedSize(List.of(record)));
        ByteBuffer payload = ByteBuffer.allocate(expected.length);
        RecordPayloadCodec.write(payload, List.of(record));
        assertArrayEquals(expected, payload.array());
        assertEquals(List.of(record), RecordPayloadCodec.read(payload.flip(), 1));
    }

    @Test
    void rejectsTrailingBytesInBoundedPayload() {
        var payload = ByteBuffer.allocate(21);
        payload.putLong(0).putInt(-1).putInt(-1).putInt(0).put((byte) 1).flip();
        assertThrows(CorruptLogException.class, () -> RecordPayloadCodec.read(payload, 1));
    }

    @Test
    void storageBatchStillMatchesLiteralVersionOneFixture() throws Exception {
        String hex;
        try (var fixture = getClass().getResourceAsStream("/storage/batch-v1.hex")) {
            assertNotNull(fixture);
            hex = new String(fixture.readAllBytes(), StandardCharsets.US_ASCII).trim();
        }
        var record = new LogRecord(7, null, new byte[0], List.of());
        assertArrayEquals(
                HexFormat.of().parseHex(hex), BatchCodec.encode(0, List.of(record), 1024));
    }
}

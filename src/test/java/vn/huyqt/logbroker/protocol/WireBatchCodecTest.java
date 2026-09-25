package vn.huyqt.logbroker.protocol;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.storage.LogRecord;

class WireBatchCodecTest {
    @Test
    void preservesRecordPayloadWithoutClientOffset() throws Exception {
        var batch = new Protocol.Batch(List.of(new LogRecord(7, null, new byte[0], List.of())));
        byte[] wire = WireBatchCodec.encode(batch, ProtocolLimits.defaults());
        assertEquals(34, wire.length);
        assertEquals("0000000000000007ffffffff0000000000000000",
                HexFormat.of().formatHex(wire, 14, wire.length));
        assertEquals(batch, WireBatchCodec.decode(wire, ProtocolLimits.defaults()));
        assertEquals(42, WireBatchCodec.fetchSize(batch));
    }

    @Test
    void rejectsCorruptionEvenWithValidLength() throws Exception {
        var batch = new Protocol.Batch(List.of(new LogRecord(0, null, null, List.of())));
        byte[] wire = WireBatchCodec.encode(batch, ProtocolLimits.defaults());
        wire[wire.length - 1] ^= 1;
        assertThrows(ProtocolException.class,
                () -> WireBatchCodec.decode(wire, ProtocolLimits.defaults()));
    }
}

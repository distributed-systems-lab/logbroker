package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PartitionerTest {
    @Test void crc32cRoutingIsStableAndUnsigned() {
        assertEquals((int) (0xe3069283L % 7),
                Partitioner.forKey("123456789".getBytes(StandardCharsets.US_ASCII), 7));
        assertEquals(0, Partitioner.forKey(new byte[0], 7));
    }
}

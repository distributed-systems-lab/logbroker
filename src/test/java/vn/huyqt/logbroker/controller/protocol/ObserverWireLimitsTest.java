package vn.huyqt.logbroker.controller.protocol;

import static org.junit.jupiter.api.Assertions.*;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.metadata.MetadataLimits;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.SenderRole;

class ObserverWireLimitsTest {
    @Test void brokerDiscoveryCodecNeedsNoLocalVoterIdentity() throws Exception {
        var frame = new Frame((short) 2, SenderRole.BROKER, (short) 106, false,
            new UUID(0, 1), 19, 7, new byte[32], new DescribeQuorum());
        byte[] encoded = QuorumCodec.encode(frame);
        var limits = QuorumCodec.WireLimits.observer(MetadataLimits.defaults());
        assertTrue(QuorumCodec.preflight(encoded, limits) >= encoded.length);
        var decoded = QuorumCodec.decode(encoded, limits);
        assertEquals(SenderRole.BROKER, decoded.senderRole());
        assertEquals(19, decoded.senderId());
        assertInstanceOf(DescribeQuorum.class, decoded.message());
    }
}

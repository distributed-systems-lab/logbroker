package vn.huyqt.logbroker.controller.transport;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;
import vn.huyqt.logbroker.support.ManualScheduler;

class ObserverBudgetTest {
    @Test
    void thirtyTwoObserversCannotConsumeVoterFrameReserve() throws Exception {
        var config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
        var control = new ResourceBudget(4096);
        var shared = new ResourceBudget(4096);
        try (var clock = new ManualScheduler();
                var occupied = shared.reserve(4096).orElseThrow()) {
            for (int broker = 1; broker <= 32; broker++) {
                var session = new Session(broker, new UUID(0, broker), new UUID(1, broker), 1);
                var frame =
                        new Frame(
                                (short) 2,
                                BrokerControlProtocol.SenderRole.BROKER,
                                (short) 112,
                                false,
                                config.identity().clusterId(),
                                broker,
                                1,
                                new byte[32],
                                new BrokerControlProtocol.ObserverFetch(
                                        session, 1, 0, 0, 4096, 100));
                var channel =
                        new EmbeddedChannel(new QuorumFrameDecoder(config, control, shared, clock));
                channel.writeInbound(Unpooled.wrappedBuffer(QuorumCodec.encode(frame)));
                assertFalse(channel.isOpen());
                channel.finishAndReleaseAll();
            }
            assertEquals(0, control.used());
            assertEquals(4096, shared.used());
            var voter = new EmbeddedChannel(new QuorumFrameDecoder(config, control, shared, clock));
            voter.writeInbound(
                    Unpooled.wrappedBuffer(
                            QuorumCodec.encode(
                                    new Frame(
                                            (short) 101,
                                            false,
                                            config.identity().clusterId(),
                                            1,
                                            7,
                                            config.identity().voterHash(),
                                            new Vote(2, 1, 3)))));
            try (var frame = (QuorumFrameDecoder.OwnedFrame) voter.readInbound()) {
                assertNotNull(frame);
            }
            voter.finishAndReleaseAll();
            assertEquals(0, control.used());
        }
        assertEquals(0, shared.used());
    }

    @Test
    void bulkReadMemoryIsBoundedAndEveryLeaseIsReleased() {
        var config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
        try (var clock = new ManualScheduler()) {
            var transport = new NettyQuorumTransport(config, clock);
            var leases = new ArrayList<ResourceBudget.Lease>();
            try {
                for (int i = 0; i < 32; i++) {
                    var lease = transport.reserveReadMemory(config.logConfig().maxBatchBytes());
                    if (lease != null) leases.add(lease);
                }
                assertTrue(leases.size() > 0 && leases.size() < 32);
                assertTrue(transport.outboundUsed() < config.outboundBytes());
                leases.forEach(ResourceBudget.Lease::close);
                assertEquals(0, transport.outboundUsed());
            } finally {
                leases.forEach(ResourceBudget.Lease::close);
                transport.closeAsync().join();
            }
        }
    }
}

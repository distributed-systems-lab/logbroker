package vn.huyqt.logbroker.controller.transport;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.Unpooled;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;

class BrokerRoleIsolationTest {
    private final ControllerConfig config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
    private Frame broker(short op, Message message) {
        return new Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER, op, false,
            config.identity().clusterId(), 1, 7, new byte[32], message);
    }
    @Test void brokerDiscoveryUsesSharedBudgetAndNeverVoterReserve() {
        var control = new ResourceBudget(65536);
        var shared = new ResourceBudget(65536);
        var channel = new EmbeddedChannel(new QuorumFrameDecoder(config, control, shared, null));
        byte[] bytes = QuorumCodec.encode(broker((short) 106, new DescribeQuorum()));
        // Split precisely before the new role byte to exercise staged header parsing.
        channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(bytes, 29)));
        channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(bytes, 29, bytes.length)));
        var owned = (QuorumFrameDecoder.OwnedFrame) channel.readInbound();
        assertNotNull(owned);
        assertFalse(owned.control());
        assertEquals(0, control.used());
        owned.close();
        channel.finishAndReleaseAll();
        assertEquals(0, shared.used());
    }
    @Test void brokerVoteAndWrongClusterCloseBeforeAllocation() {
        for (var frame : List.of(broker((short) 101, new Vote(1, 0, 0)),
            new Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER, (short) 106, false,
                new UUID(0, 999), 1, 7, new byte[32], new DescribeQuorum()))) {
            var control = new ResourceBudget(65536);
            var shared = new ResourceBudget(65536);
            var channel = new EmbeddedChannel(new QuorumFrameDecoder(config, control, shared, null));
            channel.writeInbound(Unpooled.wrappedBuffer(QuorumCodec.encode(frame)));
            assertFalse(channel.isOpen());
            assertNull(channel.readInbound());
            assertEquals(0, control.used() + shared.used());
            channel.finishAndReleaseAll();
        }
    }

    @Test void wrongHashAndInvalidRoleCloseBeforeBodyAllocation() {
        for (var frame : List.of(
            new Frame((short) 2, BrokerControlProtocol.SenderRole.VOTER, (short) 101, false,
                config.identity().clusterId(), 1, 7, new byte[32], new Vote(1, 0, 0)),
            new Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER, (short) 106, false,
                config.identity().clusterId(), 1, 7, config.identity().voterHash(), new DescribeQuorum()))) {
            var control = new ResourceBudget(65536);
            var shared = new ResourceBudget(65536);
            var channel = new EmbeddedChannel(new QuorumFrameDecoder(config, control, shared, null));
            channel.writeInbound(Unpooled.wrappedBuffer(QuorumCodec.encode(frame)));
            assertFalse(channel.isOpen());
            assertEquals(0, control.used() + shared.used());
            channel.finishAndReleaseAll();
        }
    }

    @Test void connectionCannotSwitchFromBrokerToVoterWithSameId() throws Exception {
        var received = new java.util.concurrent.LinkedBlockingQueue<QuorumTransport.Inbound>();
        try (var clock = vn.huyqt.logbroker.broker.DeadlineScheduler.system()) {
            var transport = new NettyQuorumTransport(config, clock);
            var address = transport.start(new java.net.InetSocketAddress("127.0.0.1", 0), received::add, ignored -> {});
            QuorumTransport.Inbound held = null;
            try (var socket = new java.net.Socket(address.getAddress(), address.getPort())) {
                socket.setSoTimeout(3000);
                socket.getOutputStream().write(QuorumCodec.encode(broker((short) 106, new DescribeQuorum())));
                held = received.poll(3, java.util.concurrent.TimeUnit.SECONDS);
                assertNotNull(held);
                var voter = new Frame((short) 2, BrokerControlProtocol.SenderRole.VOTER, (short) 101, false,
                    config.identity().clusterId(), 1, 8, config.identity().voterHash(), new Vote(1, 0, 0));
                socket.getOutputStream().write(QuorumCodec.encode(voter));
                assertEquals(-1, socket.getInputStream().read());
                assertNull(received.poll());
            } finally {
                if (held != null) held.close();
                transport.closeAsync().get(5, java.util.concurrent.TimeUnit.SECONDS);
            }
            assertEquals(0, transport.inboundUsed());
            assertEquals(0, transport.connectionsUsed());
        }
    }
}

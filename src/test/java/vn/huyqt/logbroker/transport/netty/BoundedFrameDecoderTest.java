package vn.huyqt.logbroker.transport.netty;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.support.ManualScheduler;

class BoundedFrameDecoderTest {
    @Test
    void fragmentedAndCoalescedFramesReturnTheirBudgetOnClose() {
        var budget = new ResourceBudget(1024);
        var channel = new EmbeddedChannel(new BoundedFrameDecoder(ProtocolLimits.defaults(), budget));
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{0, 0})));
        assertEquals(0, budget.used());
        assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{0, 0})));
        // Zero-length frame is rejected after its complete prefix.
        assertFalse(channel.isOpen());
        assertEquals(0, budget.used());
        channel.finishAndReleaseAll();
    }

    @Test
    void completeValidFrameOwnsAndReleasesOneReservation() {
        var budget = new ResourceBudget(1024);
        var channel = new EmbeddedChannel(new BoundedFrameDecoder(ProtocolLimits.defaults(), budget));
        byte[] bytes = new byte[16];
        java.nio.ByteBuffer.wrap(bytes).putInt(12);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(bytes)));
        var owned = (BoundedFrameDecoder.OwnedFrame) channel.readInbound();
        assertArrayEquals(bytes, owned.bytes());
        assertEquals(16, budget.used());
        owned.close(); owned.close();
        assertEquals(0, budget.used());
        channel.finishAndReleaseAll();
    }

    @Test
    void incompleteFrameExpiresAndReleasesReservation() {
        var budget = new ResourceBudget(1024);
        try (var clock = new ManualScheduler()) {
            var channel = new EmbeddedChannel(new BoundedFrameDecoder(
                    ProtocolLimits.defaults(), budget, clock));
            assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{0, 0, 0, 12, 1})));
            assertEquals(16, budget.used());
            clock.advance(java.time.Duration.ofSeconds(31));
            clock.runDue();
            channel.runPendingTasks();
            assertFalse(channel.isOpen());
            assertEquals(0, budget.used());
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void coalescedFramesUseSeparateLeasesAndRejectWhenBudgetIsFull() {
        var budget = new ResourceBudget(16);
        var channel = new EmbeddedChannel(new BoundedFrameDecoder(ProtocolLimits.defaults(), budget));
        byte[] together = new byte[32];
        java.nio.ByteBuffer.wrap(together).putInt(12).position(16).putInt(12);
        assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(together)));
        assertFalse(channel.isOpen());
        assertEquals(16, budget.used());
        ((BoundedFrameDecoder.OwnedFrame) channel.readInbound()).close();
        assertEquals(0, budget.used());
        channel.finishAndReleaseAll();
    }
}

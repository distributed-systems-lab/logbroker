package vn.huyqt.logbroker.transport.netty;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.ProtocolLimits;

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
}

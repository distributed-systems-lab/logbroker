package vn.huyqt.logbroker.controller.transport;
import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;
import vn.huyqt.logbroker.transport.netty.BoundedFrameDecoder;
class QuorumTransportBudgetTest {
    @Test void coalescedRawFramesAndPartialDeadlineReleaseEveryReservation() throws Exception {
        var identity=ControllerTestSupport.identity(0);var control=new ResourceBudget(2048);var shared=new ResourceBudget(2048);var clock=new vn.huyqt.logbroker.support.ManualScheduler();
        var channel=new EmbeddedChannel(new QuorumFrameDecoder(ControllerConfig.defaults(identity),control,shared,clock));
        byte[] bytes=QuorumCodec.encode(new Frame((short)101,false,identity.clusterId(),1,1,identity.voterHash(),new Vote(1,0,0)));
        var combined=ByteBuffer.allocate(bytes.length*2).put(bytes).put(bytes).array();channel.writeInbound(Unpooled.wrappedBuffer(combined));
        ((QuorumFrameDecoder.OwnedFrame)channel.readInbound()).close();((QuorumFrameDecoder.OwnedFrame)channel.readInbound()).close();assertEquals(0,control.used());
        channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(bytes,70)));assertTrue(control.used()>0);clock.advance(java.time.Duration.ofSeconds(31));clock.runDue();channel.runPendingTasks();assertFalse(channel.isOpen());assertEquals(0,control.used()+shared.used());channel.finishAndReleaseAll();clock.close();
    }
    @Test void genericDecoderUsesQuorumMinimumAndDisconnectReleasesPartialAllocation() {
        var budget=new ResourceBudget(1024);var channel=new EmbeddedChannel(new BoundedFrameDecoder(69,1024,budget,null));
        channel.writeInbound(Unpooled.wrappedBuffer(ByteBuffer.allocate(10).putInt(100).put(new byte[6]).array()));assertEquals(104,budget.used());channel.close();assertEquals(0,budget.used());channel.finishAndReleaseAll();
        var invalid=new EmbeddedChannel(new BoundedFrameDecoder(69,1024,budget,null));invalid.writeInbound(Unpooled.wrappedBuffer(ByteBuffer.allocate(4).putInt(68).array()));assertFalse(invalid.isOpen());invalid.finishAndReleaseAll();
    }
    @Test void snapshotSharedCapacityCannotConsumeReservedPeerControlBytes() throws Exception {
        var identity=ControllerTestSupport.identity(0);var config=ControllerConfig.defaults(identity);var control=new ResourceBudget(1024);var shared=new ResourceBudget(1024);
        try(var full=shared.reserve(1024).orElseThrow()) {
            var channel=new EmbeddedChannel(new QuorumFrameDecoder(config,control,shared,null));
            var frame=new Frame((short)101,false,identity.clusterId(),1,1,identity.voterHash(),new Vote(1,0,0));
            byte[] bytes=QuorumCodec.encode(frame);
            channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOf(bytes,10)));assertEquals(0,control.used());
            channel.writeInbound(Unpooled.wrappedBuffer(Arrays.copyOfRange(bytes,10,bytes.length)));
            var owned=channel.readInbound();assertInstanceOf(QuorumFrameDecoder.OwnedFrame.class,owned);assertTrue(control.used()>0);((QuorumFrameDecoder.OwnedFrame)owned).close();assertEquals(0,control.used());channel.finishAndReleaseAll();
        }
    }
    @Test void identityIsCheckedFromFixedHeaderBeforeBodyAllocation() throws Exception {
        var identity=ControllerTestSupport.identity(0);var control=new ResourceBudget(1024);var shared=new ResourceBudget(1024);
        var channel=new EmbeddedChannel(new QuorumFrameDecoder(ControllerConfig.defaults(identity),control,shared,null));
        byte[] bytes=QuorumCodec.encode(new Frame((short)101,false,new UUID(0,99),1,1,identity.voterHash(),new Vote(500,0,0)));
        channel.writeInbound(Unpooled.wrappedBuffer(bytes));assertFalse(channel.isOpen());assertEquals(0,control.used()+shared.used());channel.finishAndReleaseAll();
    }
}

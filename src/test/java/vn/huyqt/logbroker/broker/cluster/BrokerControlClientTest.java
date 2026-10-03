package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.support.ManualScheduler;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.client.NettyControllerClientTransport;

class BrokerControlClientTest {
    static BrokerClusterConfig config() { return new BrokerClusterConfig(new UUID(0,1),19,new Endpoint("localhost",19092),
        List.of(new Endpoint("localhost",19001)),Duration.ofSeconds(1),Duration.ofSeconds(2),Duration.ofSeconds(10),MetadataLimits.defaults()); }
    static BrokerControlProtocol.DescribeReply describe() {
        return new BrokerControlProtocol.DescribeReply(new ReplyMeta(QuorumError.NONE,"",1,1),
            new QuorumStatus(1,QuorumStatus.Role.LEADER,1,1,new UUID(0,8),1,1,1,1,0,true,Map.of(),""),
            ScriptedControlTransport.ID.voters(),ScriptedControlTransport.ID.voterHash());
    }
    @Test void discoversMembershipThenReusesBrokerConnection() {
        var clock=new ManualScheduler(); var factory=new ScriptedControlTransport.Factory();
        try(var client=new BrokerControlClient(config(),factory,clock)) {
            var future=client.request(new ReadMetadata(2000),2_000_000_000L);
            clock.runDue();
            assertInstanceOf(DescribeQuorum.class,factory.last().sent().getLast());
            factory.last().replyNext(describe()); clock.runDue();
            assertInstanceOf(ReadMetadata.class,factory.last().sent().getLast());
            assertEquals(BrokerControlProtocol.SenderRole.BROKER,factory.last().frames.getLast().senderRole());
            factory.last().replyNext(new BrokerControlProtocol.MetadataReply(describe().meta(),Consistency.LINEARIZABLE,1,0,MetadataImage.empty((short)2)));
            clock.runDue(); assertTrue(future.isDone()); assertFalse(future.isCompletedExceptionally());
            client.request(new DescribeQuorum(),2_000_000_000L);
            assertEquals(1,factory.connections.size());
        }
    }
    @Test void foreignDiscoveryFailsWithoutAnotherAttempt() {
        var clock=new ManualScheduler(); var factory=new ScriptedControlTransport.Factory();
        try(var client=new BrokerControlClient(config(),factory,clock)) {
            var future=client.request(new DescribeQuorum(),2_000_000_000L);
            clock.runDue();
            factory.last().replyFrom(new UUID(0,99),describe()); clock.runDue();
            assertTrue(future.isCompletedExceptionally()); assertEquals(1,factory.connections.size());
        }
    }
    @Test void retryKeepsAbsoluteDeadlineAndIgnoresOldConnection() {
        var clock=new ManualScheduler(); var factory=new ScriptedControlTransport.Factory();
        try(var client=new BrokerControlClient(config(),factory,clock,()->0.0)) {
            var future=client.request(new DescribeQuorum(),200_000_000L);
            clock.runDue();
            var old=factory.last(); old.disconnect(); clock.runDue();
            clock.advance(Duration.ofMillis(100)); clock.runDue();
            assertEquals(2,factory.connections.size()); assertTrue(old.closed);
            old.replyNext(describe()); clock.runDue(); assertFalse(future.isDone());
            clock.advance(Duration.ofMillis(100)); clock.runDue();
            assertTrue(future.isCompletedExceptionally()); assertTrue(factory.last().closed);
        }
    }
    @Test void boundsPendingCallsAndCoalescesDuplicateReplies() {
        var clock=new ManualScheduler(); var factory=new ScriptedControlTransport.Factory();
        var client=new BrokerControlClient(config(),factory,clock);
        var calls=new ArrayList<CompletableFuture<Reply>>();
        for(int i=0;i<16;i++) calls.add(client.request(new DescribeQuorum(),2_000_000_000L));
        clock.runDue(); assertTrue(client.request(new DescribeQuorum(),2_000_000_000L).isCompletedExceptionally());
        for(int i=0;i<1000;i++) factory.last().replyNext(describe());
        assertTrue(clock.pending()<=18); assertEquals(1,factory.connections.size());
        client.close(); clock.runDue(); assertTrue(calls.stream().allMatch(CompletableFuture::isCompletedExceptionally));
        assertEquals(0,clock.pending());
    }
    @Test void discoveryConsumesTheWireTimeoutBudget() {
        var clock=new ManualScheduler(); var factory=new ScriptedControlTransport.Factory();
        try(var client=new BrokerControlClient(config(),factory,clock)) {
            client.request(new ReadMetadata(2000),2_000_000_000L); clock.runDue();
            clock.advance(Duration.ofMillis(500)); factory.last().replyNext(describe()); clock.runDue();
            assertEquals(1500,assertInstanceOf(ReadMetadata.class,factory.last().sent().getLast()).timeoutMs());
        }
    }
    @Test void realNettyBrokerTransportDiscoversWithoutAConfiguredVoterIdentity() throws Exception {
        try(var server=new ServerSocket(0); var clock=DeadlineScheduler.system()) {
            var serverResult=new CompletableFuture<Void>();
            var thread=Thread.ofVirtual().start(()-> {
                try(var socket=server.accept()) {
                    socket.setSoTimeout(5000); var in=new DataInputStream(socket.getInputStream()); var out=socket.getOutputStream();
                    for(int i=0;i<2;i++) {
                        int length=in.readInt(); assertTrue(length>=70 && length<=4096);
                        byte[] raw=new byte[length+4]; java.nio.ByteBuffer.wrap(raw).putInt(length); in.readFully(raw,4,length);
                        var frame=QuorumCodec.decode(raw,QuorumCodec.WireLimits.observer(MetadataLimits.defaults()));
                        assertEquals(BrokerControlProtocol.SenderRole.BROKER,frame.senderRole());
                        Reply reply=i==0 ? describe() : new BrokerControlProtocol.MetadataReply(describe().meta(),Consistency.LINEARIZABLE,1,0,MetadataImage.empty((short)2));
                        out.write(QuorumCodec.encode(new Frame((short)2,BrokerControlProtocol.SenderRole.VOTER,frame.operation(),true,
                            config().clusterId(),1,frame.requestId(),ScriptedControlTransport.ID.voterHash(),reply))); out.flush();
                    }
                    serverResult.complete(null);
                } catch(Throwable e) { serverResult.completeExceptionally(e); }
            });
            var cfg=new BrokerClusterConfig(config().clusterId(),19,config().advertised(),List.of(new Endpoint("localhost",server.getLocalPort())),
                config().heartbeatInterval(),config().heartbeatRpcTimeout(),config().sessionTimeout(),config().limits());
            try(var client=new BrokerControlClient(cfg,new NettyControllerClientTransport.Factory(cfg,clock),clock)) {
                assertInstanceOf(BrokerControlProtocol.MetadataReply.class,client.request(new ReadMetadata(2000),clock.nanoTime()+5_000_000_000L).get(5,TimeUnit.SECONDS));
                serverResult.get(5,TimeUnit.SECONDS);
            } finally { server.close(); thread.join(6000); assertFalse(thread.isAlive()); }
        }
    }
}

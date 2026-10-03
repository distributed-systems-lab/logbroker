package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.support.QuorumHarness;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.*;

class BrokerFencingTest {
    @Test void minorityCannotAcknowledgeUnfenceAndRetrySharesCommittedRevision() {
        var h = leader(); var s = register(h); var recovery = new UUID(0,9);
        heartbeat(h,0,s,1,0,recovery,false);
        h.pauseDisk(1); h.pauseDisk(2);
        var request = new Heartbeat(s,2,h.image(0).appliedOffset(),recovery,true,2000);
        var f = h.service(0).request(request,h.now()+2_000_000_000L); h.settle();
        assertFalse(f.isDone()); assertTrue(h.image(0).brokers().get(1).fenced());
        h.resumeDisk(1); h.settle();
        var allowed = (HeartbeatReply)f.join(); assertEquals(SessionStatus.ACTIVE,allowed.status());
        long end = h.node(0).status().logEnd();
        var retry = heartbeat(h,0,s,2,h.image(0).appliedOffset(),recovery,true);
        assertEquals(allowed.stateOffset(),retry.stateOffset()); assertEquals(end,h.node(0).status().logEnd());
    }
    @Test void freshHeartbeatCannotWithdrawPendingFenceAndRecoveryWritesNewRevision() {
        var h = leader(); var s = register(h); var recovery = new UUID(0,9);
        heartbeat(h,0,s,1,0,recovery,false); heartbeat(h,0,s,2,h.image(0).appliedOffset(),recovery,true);
        long previous = h.image(0).brokers().get(1).stateOffset();
        for (int i=0;i<99;i++) { h.tick(Duration.ofMillis(100)); h.settle(); }
        h.pauseDisk(0); h.tick(Duration.ofMillis(100)); h.settle();
        assertTrue(h.disk(0).pending()>0);
        heartbeat(h,0,s,3,h.image(0).appliedOffset(),new UUID(0,10),true);
        h.resumeDisk(0); h.settle();
        assertTrue(h.image(0).brokers().get(1).fenced());
        long fence = h.image(0).brokers().get(1).stateOffset(); assertTrue(fence>previous);
        var target = heartbeat(h,0,s,4,0,new UUID(0,11),false).requiredMetadataOffset();
        var allowed = heartbeat(h,0,s,5,target,new UUID(0,11),true);
        assertEquals(SessionStatus.ACTIVE,allowed.status()); assertTrue(allowed.stateOffset()>fence);
    }
    @Test void leaderChangeRequiresFreshContactForAssignmentAndFreshUnfenceIsAtomic() {
        var h = leader(); var s = register(h); var recovery = new UUID(0,9);
        heartbeat(h,0,s,1,0,recovery,false); heartbeat(h,0,s,2,h.image(0).appliedOffset(),recovery,true);
        h.isolate(0); h.tickNode(1,Duration.ofSeconds(4)); h.settle();
        var rejected = h.service(1).request(new CreateTopic("orders",2,(short)1,2000),h.now()+2_000_000_000L); h.settle();
        assertTrue(rejected.isCompletedExceptionally());
        heartbeat(h,1,s,1,h.image(1).appliedOffset(),new UUID(0,0),false);
        var created = h.service(1).request(new CreateTopic("orders",2,(short)1,2000),h.now()+2_000_000_000L); h.settle();
        assertNotNull(created.join());
        long oldRevision = h.image(1).brokers().get(1).stateOffset();
        var fresh = heartbeat(h,1,s,2,h.image(1).appliedOffset(),new UUID(0,10),true);
        assertTrue(fresh.stateOffset()>oldRevision);
        assertTrue(h.image(1).partitions().values().stream().allMatch(p -> p.leaderEpoch()==1 && p.partitionEpoch()==1));
        var last = h.disk(1).batches().getLast(); assertEquals(3,last.entries().size());
    }
    @Test void staleSessionCannotRenewLivenessOrUnfence() {
        var h = leader(); var s = register(h);
        var wrong = new Session(1,s.storageId(),new UUID(0,99),s.brokerEpoch());
        long end = h.node(0).status().logEnd();
        assertEquals(SessionStatus.STALE_SESSION,heartbeat(h,0,wrong,1,end,new UUID(0,9),true).status());
        assertEquals(end,h.node(0).status().logEnd());
    }
    private static QuorumHarness leader() {
        var h = QuorumHarness.threeNodes(7,(short)2); h.tickNode(0,Duration.ofSeconds(4)); h.settle(); return h;
    }
    private static Session register(QuorumHarness h) {
        var f = h.service(0).request(new Register(1,new UUID(0,1),new UUID(0,2),-1,
            new Endpoint("localhost",9092),(short)2,(short)2,2000),h.now()+2_000_000_000L);
        h.settle(); return ((RegisterReply)f.join()).session();
    }
    private static HeartbeatReply heartbeat(QuorumHarness h, int node, Session s,long seq,long applied,UUID recovery,boolean complete) {
        var f = h.service(node).request(new Heartbeat(s,seq,applied,recovery,complete,2000),h.now()+2_000_000_000L);
        h.settle(); return (HeartbeatReply)f.join();
    }
    @Test void steadyHeartbeatsDoNotAppendAndRecoveryTargetIsFixed() {
        var h = leader(); var s = register(h); var recovery = new UUID(0,9);
        var first = heartbeat(h,0,s,1,0,recovery,false);
        long target = first.requiredMetadataOffset(); assertEquals(SessionStatus.FENCED,first.status());
        h.appendBarrier(0); h.settle();
        var second = heartbeat(h,0,s,2,target-1,recovery,false);
        assertEquals(target,second.requiredMetadataOffset());
        var active = heartbeat(h,0,s,3,target,recovery,true);
        assertEquals(SessionStatus.ACTIVE,active.status()); assertTrue(active.stateOffset() > target);
        long end = h.node(0).status().logEnd();
        for (long seq=4;seq<20;seq++) heartbeat(h,0,s,seq,h.image(0).appliedOffset(),recovery,true);
        assertEquals(end,h.node(0).status().logEnd());
    }
}

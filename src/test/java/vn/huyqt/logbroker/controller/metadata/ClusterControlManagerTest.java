package vn.huyqt.logbroker.controller.metadata;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.log.*;

class ClusterControlManagerTest {
    @Test void replacementCannotRacePendingUnfence() throws Exception {
        var state = ClusterMetadataApplyTest.initialized();
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true); manager.onImage(state.image());
        manager.leaderStarted(new HeartbeatTracker(10),0,0);
        var session = state.image().brokers().get(1).registration().session();
        var meta = new QuorumProtocol.ReplyMeta(QuorumError.NONE,"",0,0);
        assertTrue(manager.heartbeat(1,new BrokerControlProtocol.Heartbeat(session,1,2,new UUID(0,9),true,2000),0,meta).isEmpty());
        assertEquals(QuorumError.BROKER_ID_IN_USE,manager.submit(2,new BrokerControlProtocol.Register(1,
            session.storageId(),new UUID(0,99),2,state.image().brokers().get(1).registration().endpoint(),(short)2,(short)2,2000)));
    }
    @Test void lifecycleDrainWaitsForPriorTopicImageSoAllAssignmentEpochsAdvance() throws Exception {
        var state = servingState(); var manager = new ClusterControlManager(MetadataLimits.defaults(),id -> true);
        manager.onImage(state.image()); manager.leaderStarted(new HeartbeatTracker(10),0,0);
        assertEquals(QuorumError.NONE,manager.submit(1,new BrokerControlProtocol.CreateTopic("a",2,(short)1,2000)));
        var s = state.image().brokers().get(1).registration().session();
        var meta = new QuorumProtocol.ReplyMeta(QuorumError.NONE,"",0,0);
        assertTrue(manager.heartbeat(2,new BrokerControlProtocol.Heartbeat(s,1,3,new UUID(0,9),true,2000),0,meta).isEmpty());
        var topic = manager.drainNext(0,3).orElseThrow();
        assertTrue(manager.drainNext(0,topic.nextOffset()).isEmpty());
        state.apply(topic); manager.onImage(state.image());
        var lifecycle = manager.drainNext(0,topic.nextOffset()).orElseThrow(); assertEquals(3,lifecycle.entries().size());
        state.apply(lifecycle); assertTrue(state.image().partitions().values().stream().allMatch(p -> p.leaderEpoch()==1));
    }
    private static BrokerControlProtocol.Register registration(int id, long expected, long incarnation) {
        return new BrokerControlProtocol.Register(id, new UUID(0,id), new UUID(0,incarnation), expected,
            new ClusterRecords.Endpoint("localhost", 9092 + id), (short) 2, (short) 2, 2000);
    }
    private static MetadataStateMachine servingState() throws Exception {
        var state = ClusterMetadataApplyTest.initialized();
        state.apply(new QuorumBatch(2, List.of(new QuorumEntry.BrokerState(0, new ClusterRecords.BrokerState(1,2,false)))));
        return state;
    }
    @Test void registrationChecksStorageCompatibilityCasAndCommittedFence() throws Exception {
        var state = servingState();
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true); manager.onImage(state.image());
        var old = state.image().brokers().get(1).registration();
        assertEquals(QuorumError.BROKER_ID_IN_USE, manager.submit(1, new BrokerControlProtocol.Register(1,
            old.session().storageId(), new UUID(0,99), 2, old.endpoint(), (short)2, (short)2, 2000)));
        assertEquals(QuorumError.STORAGE_ID_MISMATCH, manager.submit(2, new BrokerControlProtocol.Register(1,
            new UUID(0,999), new UUID(0,99), 2, old.endpoint(), (short)2, (short)2, 2000)));
        assertEquals(QuorumError.INCOMPATIBLE_METADATA_VERSION, manager.submit(3, new BrokerControlProtocol.Register(2,
            new UUID(0,2), new UUID(0,3), -1, old.endpoint(), (short)3, (short)3, 2000)));
        state.apply(new QuorumBatch(3, List.of(new QuorumEntry.BrokerState(0, new ClusterRecords.BrokerState(1,2,true)))));
        manager.onImage(state.image());
        assertEquals(QuorumError.NONE, manager.submit(4, new BrokerControlProtocol.Register(1,
            old.session().storageId(), new UUID(0,99), 2, old.endpoint(), (short)2, (short)2, 2000)));
        var replacement = manager.drainNext(0,4).orElseThrow(); state.apply(replacement); manager.onImage(state.image());
        assertEquals(5, ((BrokerControlProtocol.RegisterReply)manager.completions(new QuorumProtocol.ReplyMeta(QuorumError.NONE,"",0,0)).getFirst().reply()).session().brokerEpoch());
    }
    @Test void pendingCapacityAndConflictsDoNotSplitOrReserveRejectedCommands() throws Exception {
        var state = servingState();
        var limits = new MetadataLimits(1,1,4,4096);
        var manager = new ClusterControlManager(limits, id -> true); manager.onImage(state.image());
        assertEquals(QuorumError.OVERLOADED, manager.submit(1, registration(2,-1,9)));
        var create = new BrokerControlProtocol.CreateTopic("a",4,(short)1,2000);
        assertEquals(QuorumError.NONE, manager.submit(2, create));
        assertEquals(QuorumError.NONE, manager.submit(3, create));
        assertEquals(QuorumError.TOPIC_ALREADY_EXISTS, manager.submit(4, new BrokerControlProtocol.CreateTopic("a",3,(short)1,2000)));
        assertEquals(QuorumError.OVERLOADED, manager.submit(5, new BrokerControlProtocol.CreateTopic("b",1,(short)1,2000)));
        assertEquals(5, manager.drainNext(0,3).orElseThrow().entries().size());
        assertTrue(manager.drainNext(0,8).isEmpty());
        manager.onLeadershipLost();
        assertEquals(QuorumError.NONE, manager.submit(6, create));
    }
    @Test void maximumTopicFitsOneBatchAndCancellationDoesNotUndoAdmittedMutation() throws Exception {
        var state = servingState();
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true); manager.onImage(state.image());
        var create = new BrokerControlProtocol.CreateTopic("maximum",1024,(short)1,2000);
        assertEquals(QuorumError.NONE, manager.submit(1, create)); manager.cancel(1);
        var batch = manager.drainNext(0,3).orElseThrow();
        assertEquals(1025, batch.entries().size()); assertTrue(ClusterControlManager.storageSize(batch.entries()) <= 1024*1024);
        state.apply(batch); manager.onImage(state.image());
        assertTrue(manager.completions(new QuorumProtocol.ReplyMeta(QuorumError.NONE,"",0,0)).isEmpty());
        assertEquals(1024, state.image().partitions().size());
        assertEquals(QuorumError.NONE, manager.submit(2, create));
        assertInstanceOf(QuorumEntry.ReadBarrier.class, manager.drainNext(0,batch.nextOffset()).orElseThrow().entries().getFirst());
    }
    @Test void tooSmallBatchBudgetRejectsWholeCommand() throws Exception {
        var state = servingState();
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true, 100); manager.onImage(state.image());
        assertEquals(QuorumError.BATCH_TOO_LARGE, manager.submit(1,new BrokerControlProtocol.CreateTopic("a",2,(short)1,2000)));
        assertTrue(manager.drainNext(0,3).isEmpty());
    }
    @Test void allocationAccountsForPendingAndTieBreaksById() {
        assertEquals(List.of(2,3,2,3,1), ClusterControlManager.assign(List.of(1,2,3), Map.of(1,2,2,0,3,0), 5));
    }
    @Test void registrationRetriesShareOneExactOffsetAndCompleteOnlyAfterApply() throws Exception {
        var state = new MetadataStateMachine(MetadataLimits.defaults(), (short) 2);
        state.apply(ClusterMetadataApplyTest.featureBatch());
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true);
        manager.onImage(state.image());
        var request = new BrokerControlProtocol.Register(1, new UUID(0,1), new UUID(0,2), -1,
            new ClusterRecords.Endpoint("localhost", 9092), (short) 2, (short) 2, 2000);
        assertEquals(QuorumError.NONE, manager.submit(1, request));
        assertEquals(QuorumError.NONE, manager.submit(2, request));
        var batch = manager.drainNext(0, 1).orElseThrow();
        assertEquals(2, ((QuorumEntry.BrokerRegistration) batch.entries().getFirst()).event().session().brokerEpoch());
        assertTrue(manager.drainNext(0, 2).isEmpty());
        assertTrue(manager.completions(new QuorumProtocol.ReplyMeta(QuorumError.NONE, "", 0, 0)).isEmpty());
        state.apply(batch); manager.onImage(state.image());
        assertEquals(2, manager.completions(new QuorumProtocol.ReplyMeta(QuorumError.NONE, "", 0, 0)).size());
        assertEquals(QuorumError.STALE_BROKER_EPOCH, manager.submit(3,
            new BrokerControlProtocol.Register(1, request.storageId(), new UUID(0,3), -1,
                request.endpoint(), (short) 2, (short) 2, 2000)));
    }
    @Test void emptyEligibilityRejectsCreateWithoutReservation() throws Exception {
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true);
        var state = new MetadataStateMachine(MetadataLimits.defaults(), (short) 2);
        state.apply(ClusterMetadataApplyTest.featureBatch()); manager.onImage(state.image());
        assertEquals(QuorumError.NO_ELIGIBLE_BROKER, manager.submit(1, new BrokerControlProtocol.CreateTopic("orders", 2, (short) 1, 2000)));
        assertTrue(manager.drainNext(0, 1).isEmpty());
    }
    @Test void concurrentTopicsCountPendingAssignmentsAndRemainWholeBatches() throws Exception {
        var state = ClusterMetadataApplyTest.initialized();
        state.apply(new QuorumBatch(2, List.of(new QuorumEntry.BrokerRegistration(0,
            new ClusterRecords.BrokerRegistration(new ClusterRecords.Session(2, new UUID(0,3), new UUID(0,4), 3),
                new ClusterRecords.Endpoint("localhost", 9093), (short) 2, (short) 2)))));
        state.apply(new QuorumBatch(3, List.of(new QuorumEntry.BrokerState(0, new ClusterRecords.BrokerState(1,2,false)),
            new QuorumEntry.BrokerState(0, new ClusterRecords.BrokerState(2,3,false)))));
        var manager = new ClusterControlManager(MetadataLimits.defaults(), id -> true); manager.onImage(state.image());
        assertEquals(QuorumError.NONE, manager.submit(1, new BrokerControlProtocol.CreateTopic("a",3,(short) 1,2000)));
        assertEquals(QuorumError.NONE, manager.submit(2, new BrokerControlProtocol.CreateTopic("b",1,(short) 1,2000)));
        var a = manager.drainNext(0,5).orElseThrow(); var b = manager.drainNext(0,a.nextOffset()).orElseThrow();
        assertEquals(4,a.entries().size()); assertEquals(2,b.entries().size());
        assertEquals(2, ((QuorumEntry.PartitionRecord) b.entries().getLast()).event().leaderId());
        state.apply(a); state.apply(b); manager.onImage(state.image());
        assertEquals(2, manager.completions(new QuorumProtocol.ReplyMeta(QuorumError.NONE,"",0,0)).size());
    }
}

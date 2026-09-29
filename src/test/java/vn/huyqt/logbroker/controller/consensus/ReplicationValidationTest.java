package vn.huyqt.logbroker.controller.consensus;
import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;
class ReplicationValidationTest {
    private QuorumStateMachine node(QuorumStatus.Role role,long start,List<QuorumBatch> batches) {
        var index=new EpochIndex(start,1,batches);
        var status=new QuorumStatus(0,role,1,role==QuorumStatus.Role.LEADER?0:1,new UUID(0,10),index.end(),index.end(),start,start,start,false,Map.of(),"");
        return new QuorumStateMachine(ControllerConfig.defaults(ControllerTestSupport.identity(0)),status,index,-1,new Random(1),0);
    }
    private PeerRequest fetch(long id,long end,long last,long challenge) {return new PeerRequest(ElectionTest.frame(1,id,new QuorumFetch(1,end,last,1024*1024,0,challenge)),new ReplyRoute(1,id));}
    @Test void unavailablePrefixRequiresPublishedSnapshot() {
        var n=node(QuorumStatus.Role.LEADER,5,List.of());var snapshot=new SnapshotId(5,1,new UUID(0,99));n.on(new SnapshotAvailable(snapshot));
        var effects=n.on(fetch(1,0,0,0));var reply=(QuorumFetchReply)((QuorumEffect.Reply)effects.getFirst()).frame().message();
        assertEquals(new SnapshotRequired(snapshot),reply.payload());
    }
    @Test void offsetBeyondLeaderEndProducesDivergenceRatherThanDurableMatch() {
        var n=node(QuorumStatus.Role.LEADER,0,List.of(new QuorumBatch(0,List.of(new QuorumEntry.ReadBarrier(1)))));
        var effects=n.on(fetch(1,50,2,0));
        var reply=(QuorumFetchReply)((QuorumEffect.Reply)effects.getFirst()).frame().message();
        assertEquals(new Divergence(1,1),reply.payload());assertTrue(n.status().durableMatches().isEmpty());
    }
    @Test void replayedChallengeCannotExtendLeaderContactTwice() {
        var n=node(QuorumStatus.Role.LEADER,0,List.of());
        var work=(QuorumEffect.ReadLog)n.on(fetch(1,0,1,0)).getFirst();
        var first=n.on(new DiskDone(work.token(),new DiskResult.Read(List.of())));
        long challenge=((QuorumFetchReply)((QuorumEffect.Reply)first.getFirst()).frame().message()).challenge();
        n.on(new Tick(2_000_000_000L));
        var next=(QuorumEffect.ReadLog)n.on(fetch(2,0,1,challenge)).getFirst();
        n.on(new DiskDone(next.token(),new DiskResult.Read(List.of())));
        n.on(new Tick(4_000_000_000L));n.on(fetch(3,0,1,challenge));n.on(new Tick(5_100_000_000L));
        assertNotEquals(QuorumStatus.Role.LEADER,n.status().role());
    }
    @Test void uncorrelatedHigherEpochResponseCannotStepDownOrRestoreProgress() {
        var n=node(QuorumStatus.Role.LEADER,0,List.of());var id=ControllerTestSupport.identity(0);
        n.on(new PeerResponse(new Frame((short)104,true,id.clusterId(),1,999,id.voterHash(),new QuorumFetchReply(new ReplyMeta(QuorumError.NONE,"",500,1),1,100,new FetchData(List.of())))));
        assertEquals(1,n.status().epoch());assertEquals(0,n.status().commit());assertEquals(QuorumStatus.Role.LEADER,n.status().role());
    }
    @Test void followerCommitClampsToItsDurableWholeBatchBoundary() {
        var n=node(QuorumStatus.Role.FOLLOWER,0,List.of(new QuorumBatch(0,List.of(new QuorumEntry.ReadBarrier(1)))));
        var send=(QuorumEffect.Send)n.on(new Tick(0)).getFirst();var id=ControllerTestSupport.identity(0);
        n.on(new PeerResponse(new Frame((short)104,true,id.clusterId(),1,send.frame().requestId(),id.voterHash(),new QuorumFetchReply(new ReplyMeta(QuorumError.NONE,"",1,1),1,100,new FetchData(List.of())))));
        assertEquals(1,n.status().commit());assertEquals(0,n.status().applied());
    }
    @Test void lateFetchFromPreviousEpochCannotAppendOrAdvanceCommit() {
        var n=node(QuorumStatus.Role.FOLLOWER,0,List.of());
        var send=(QuorumEffect.Send)n.on(new Tick(0)).getFirst();var id=ControllerTestSupport.identity(0);
        n.on(new PeerRequest(ElectionTest.frame(2,99,new BeginQuorumEpoch(2)),new ReplyRoute(2,99)));
        var stale=new QuorumBatch(0,List.of(new QuorumEntry.ReadBarrier(1)));
        var effects=n.on(new PeerResponse(new Frame((short)104,true,id.clusterId(),1,send.frame().requestId(),id.voterHash(),new QuorumFetchReply(new ReplyMeta(QuorumError.NONE,"",1,1),1,1,new FetchData(List.of(stale))))));
        assertTrue(effects.isEmpty());assertEquals(0,n.status().logEnd());assertEquals(0,n.status().commit());assertEquals(2,n.status().epoch());
    }
}

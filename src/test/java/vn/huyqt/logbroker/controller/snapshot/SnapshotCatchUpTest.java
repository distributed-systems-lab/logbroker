package vn.huyqt.logbroker.controller.snapshot;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.*;
class SnapshotCatchUpTest {
    @Test void laggingFollowerInstallsSnapshotAndRejoinsPullReplication() {
        var h=QuorumHarness.threeNodes(1);h.elect(0);h.isolate(2);
        for(int i=0;i<5;i++){var create=h.service(0).createTopic("topic"+i,1,h.now()+30_000_000_000L);h.settle();if(!create.isDone()){h.tickNode(0,Duration.ofMillis(100));h.tickNode(1,Duration.ZERO);h.settle();}assertNotNull(create.orTimeout(2,java.util.concurrent.TimeUnit.SECONDS).join());}
        h.compact(0);long snapshotEnd=h.node(0).status().snapshotEnd();var oldGeneration=h.node(2).status().generation();h.heal();
        var identity=ControllerTestSupport.identity(0);var begin=new BeginQuorumEpoch(h.node(0).status().epoch());
        h.request(2,new Frame((short)102,false,identity.clusterId(),0,1000,identity.voterHash(),begin));h.settle();
        assertNotEquals(oldGeneration,h.node(2).status().generation());assertEquals(snapshotEnd,h.node(2).status().applied());
        var local=h.service(2).readLocalMetadata();h.settle();assertEquals(5,local.join().topics().size());
        h.appendBarrier(0);h.settle();h.tick(Duration.ofMillis(200));h.settle();assertTrue(h.node(2).status().durableEnd()>snapshotEnd);h.assertSafety();
    }
    @Test void delayedAppliedFromOldGenerationCannotReplaceSnapshotImage() {
        var generation=new UUID(0,100);var index=new EpochIndex(10,2,List.of());
        var status=new QuorumStatus(1,QuorumStatus.Role.FOLLOWER,3,0,generation,10,10,10,10,10,false,Map.of(),"");
        var node=new QuorumStateMachine(ControllerConfig.defaults(ControllerTestSupport.identity(1)),status,index,-1,new Random(1),0);
        assertTrue(node.on(new QuorumEvent.Applied(1,new MetadataImage(1,List.of()),new UUID(0,11))).isEmpty());assertEquals(10,node.status().applied());
    }
}

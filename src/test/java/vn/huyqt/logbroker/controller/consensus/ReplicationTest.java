package vn.huyqt.logbroker.controller.consensus;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.support.QuorumHarness;
class ReplicationTest {
    @Test void appendedMajorityDoesNotCountUntilFollowerFlushes() {
        var h=QuorumHarness.threeNodes(1);h.elect(0);long before=h.node(0).status().commit();
        h.pauseDisk(1);h.pauseDisk(2);h.appendBarrier(0);h.settle();
        h.completeOneDisk(1);h.deliverAll();
        assertTrue(h.node(1).status().logEnd()>h.node(1).status().durableEnd());
        assertEquals(before,h.node(0).status().commit());
        h.resumeDisk(1);h.settle();assertTrue(h.node(0).status().commit()>before);
    }
    @Test void commitWaitsForDurableFollowerAndThenAppliesMarker() {
        var h=QuorumHarness.threeNodes(1);h.elect(0);
        assertTrue(h.node(0).status().ready());long before=h.node(0).status().commit();
        h.pauseDisk(1);h.pauseDisk(2);h.appendBarrier(0);h.settle();
        assertEquals(before,h.node(0).status().commit());
        h.resumeDisk(1);h.settle();assertTrue(h.node(0).status().commit()>before);h.assertSafety();
    }
    @Test void isolationExpiresLeadershipAndHealingElectsCompleteLog() {
        var h=QuorumHarness.threeNodes(4);h.elect(0);h.isolate(0);
        h.tick(Duration.ofSeconds(4));h.settle();assertNotEquals(QuorumStatus.Role.LEADER,h.node(0).status().role());
        h.heal();for(int i=0;i<100&&h.leader()<0;i++){h.tick(Duration.ofMillis(50));h.settle();}
        assertTrue(h.leader()>=0);h.assertSafety();
    }
    @Test void forceFailureStopsVotingAndReplication() {
        var h=QuorumHarness.threeNodes(2);h.elect(0);h.disk(0).failNextForce();h.appendBarrier(0);h.settle();
        assertEquals(QuorumStatus.Role.FAILED,h.node(0).status().role());
    }
}

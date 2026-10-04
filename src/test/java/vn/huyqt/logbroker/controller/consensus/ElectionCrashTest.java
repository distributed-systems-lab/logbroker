package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.QuorumHarness;

class ElectionCrashTest {
    @Test
    void crashedCandidateKeepsSelfVoteOnRestart() {
        var h = QuorumHarness.threeNodes(4);
        h.isolate(1);
        h.tick(java.time.Duration.ofSeconds(4));
        h.completeDisks();
        assertEquals(1, h.disk(1).votedFor());
        long epoch = h.disk(1).epoch();
        h.crash(1);
        h.restart(1);
        h.heal();
        h.request(1, ElectionTest.frame(0, 8, new Vote(epoch, 0, 0)));
        h.settle();
        assertEquals(1, h.disk(1).votedFor());
    }

    @Test
    void restartCannotGrantSecondVoteInSameEpoch() {
        var h = QuorumHarness.threeNodes(3);
        h.request(1, ElectionTest.frame(0, 1, new Vote(2, 0, 0)));
        h.settle();
        assertEquals(0, h.disk(1).votedFor());
        h.crash(1);
        h.restart(1);
        h.request(1, ElectionTest.frame(2, 2, new Vote(2, 0, 0)));
        h.settle();
        assertEquals(0, h.disk(1).votedFor());
    }

    @Test
    void higherEpochWhileVoteForcePendingCannotProduceOldGrantedResponse() {
        var h = QuorumHarness.threeNodes(7);
        h.pauseDisk(1);
        h.request(1, ElectionTest.frame(0, 1, new Vote(2, 0, 0)));
        h.request(1, ElectionTest.frame(2, 2, new Vote(3, 0, 0)));
        assertEquals(3, h.node(1).status().epoch());
        h.resumeDisk(1);
        h.settle();
        assertEquals(3, h.disk(1).epoch());
        assertEquals(2, h.disk(1).votedFor());
        assertTrue(
                h.transport().history().stream()
                        .noneMatch(
                                e ->
                                        e.senderId() == 1
                                                && e.message() instanceof VoteReply v
                                                && v.granted()
                                                && v.meta().epoch() == 2));
    }
}

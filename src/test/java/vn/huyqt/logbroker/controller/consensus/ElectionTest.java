package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.*;

import java.time.Duration;
import java.util.*;

class ElectionTest {
    @Test
    void grantedVoteIsNotSentUntilBothEpochAndVoteAreForced() {
        var h = QuorumHarness.threeNodes(2);
        h.pauseDisk(1);
        h.request(1, frame(0, 7, new Vote(1, 0, 0)));
        assertTrue(h.transport().history().isEmpty());
        h.resumeDisk(1);
        h.completeDisks();
        assertTrue(h.transport().history().isEmpty());
        h.completeDisks();
        assertTrue(
                h.transport().history().stream()
                        .anyMatch(f -> f.message() instanceof VoteReply v && v.granted()));
    }

    @Test
    void staleEpochCannotChangeRoleOrVote() {
        var h = QuorumHarness.threeNodes(8);
        h.request(1, frame(0, 1, new Vote(3, 0, 0)));
        h.settle();
        h.request(1, frame(2, 2, new Vote(2, 0, 0)));
        h.settle();
        assertEquals(3, h.node(1).status().epoch());
        assertEquals(0, h.disk(1).votedFor());
        assertTrue(
                h.transport().history().stream()
                        .anyMatch(
                                f ->
                                        f.message() instanceof VoteReply v
                                                && v.meta().error()
                                                        == vn.huyqt.logbroker.controller.protocol
                                                                .QuorumError.STALE_EPOCH));
    }

    @Test
    void selfVoteMustBeForcedBeforeSolicitingVotes() {
        var h = QuorumHarness.threeNodes(42);
        h.tick(Duration.ofSeconds(4));
        assertTrue(h.transport().voteRequests().isEmpty());
        h.completeDisks();
        h.completeDisks();
        h.deliverAll();
        assertFalse(h.transport().voteRequests().isEmpty());
        h.assertSafety();
    }

    @Test
    void electionAppendsMarkerButCannotAdvertiseReadinessYet() {
        var h = QuorumHarness.threeNodes(9);
        h.tickNode(0, Duration.ofSeconds(4));
        for (int i = 0; i < 10 && h.leader() < 0; i++) {
            h.completeDisks();
            h.deliverAll();
        }
        h.pauseDisk(1);
        h.pauseDisk(2);
        h.settle();
        assertEquals(0, h.leader());
        assertFalse(h.node(0).status().ready());
        assertInstanceOf(
                QuorumEntry.LeaderChange.class,
                h.disk(0).batches().getFirst().entries().getFirst());
    }

    @Test
    void candidateWithLongerButOlderEpochLogIsRejectedAfterHardStateForce() {
        var batch = new QuorumBatch(0, List.of(new QuorumEntry.ReadBarrier(4)));
        var index = new EpochIndex(0, 0, List.of(batch));
        var s =
                new QuorumStatus(
                        1,
                        QuorumStatus.Role.UNATTACHED,
                        4,
                        -1,
                        new UUID(0, 11),
                        1,
                        1,
                        0,
                        0,
                        0,
                        false,
                        Map.of(),
                        "");
        var node =
                new QuorumStateMachine(
                        ControllerConfig.defaults(ControllerTestSupport.identity(1)),
                        s,
                        index,
                        -1,
                        new Random(1),
                        0);
        var request =
                new QuorumEvent.PeerRequest(frame(0, 1, new Vote(5, 3, 100)), new ReplyRoute(0, 1));
        var effects = node.on(request);
        assertTrue(effects.stream().noneMatch(QuorumEffect.Reply.class::isInstance));
        var persist = (QuorumEffect.PersistVote) effects.getFirst();
        var after =
                node.on(
                        new QuorumEvent.DiskDone(
                                persist.token(), new QuorumEvent.DiskResult.VoteSaved(5, -1)));
        var reply = (VoteReply) ((QuorumEffect.Reply) after.getFirst()).frame().message();
        assertFalse(reply.granted());
    }

    @Test
    void duplicateBeginIsAcceptedButConflictingSameEpochLeaderIsRejected() {
        var h = QuorumHarness.threeNodes(1);
        h.request(1, frame(0, 1, new BeginQuorumEpoch(3)));
        h.settle();
        assertEquals(0, h.node(1).status().leaderId());
        h.request(1, frame(0, 2, new BeginQuorumEpoch(3)));
        h.settle();
        h.request(1, frame(2, 3, new BeginQuorumEpoch(3)));
        h.settle();
        assertEquals(0, h.node(1).status().leaderId());
    }

    @Test
    void splitVoteRetriesWithInjectedJitter() {
        var h = QuorumHarness.threeNodes(42);
        h.tick(Duration.ofSeconds(4));
        h.settle();
        assertEquals(-1, h.leader());
        for (int i = 0; i < 100 && h.leader() < 0; i++) {
            h.tick(Duration.ofMillis(50));
            h.settle();
        }
        assertTrue(h.leader() >= 0);
        h.assertSafety();
    }

    static Frame frame(int sender, long id, Request request) {
        var identity = ControllerTestSupport.identity(0);
        return new Frame(
                vn.huyqt.logbroker.controller.protocol.QuorumProtocol.operation(request),
                false,
                identity.clusterId(),
                sender,
                id,
                identity.voterHash(),
                request);
    }
}

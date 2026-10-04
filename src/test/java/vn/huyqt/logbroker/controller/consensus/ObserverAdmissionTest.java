package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.support.QuorumHarness;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

class ObserverAdmissionTest {
    @Test
    void retentionBetweenAdmissionAndReadReturnsRetryableSnapshotFailure() {
        var h = leader();
        var s = register(h);
        h.pauseDisk(0);
        var fetch =
                h.service(0)
                        .request(
                                new ObserverFetch(s, h.node(0).status().epoch(), 0, 0, 4096, 0),
                                h.now() + 2_000_000_000L);
        h.settle();
        h.compact(0);
        h.resumeDisk(0);
        h.settle();
        assertTrue(fetch.isCompletedExceptionally());
        assertEquals(QuorumStatus.Role.LEADER, h.node(0).status().role());
        var retry =
                h.service(0)
                        .request(
                                new ObserverFetch(s, h.node(0).status().epoch(), 0, 0, 4096, 0),
                                h.now() + 2_000_000_000L);
        h.settle();
        assertInstanceOf(SnapshotRequired.class, ((ObserverFetchReply) retry.join()).payload());
    }

    @Test
    void thirtyTwoParkedObserversDoNotPreventMajorityCommit() {
        var h = leader();
        var sessions = new ArrayList<Session>();
        for (int id = 1; id <= 32; id++) {
            var f =
                    h.service(0)
                            .request(
                                    new Register(
                                            id,
                                            new UUID(0, id),
                                            new UUID(1, id),
                                            -1,
                                            new Endpoint("localhost", 9092 + id),
                                            (short) 2,
                                            (short) 2,
                                            2000),
                                    h.now() + 2_000_000_000L);
            h.settle();
            sessions.add(((RegisterReply) f.join()).session());
        }
        long before = h.node(0).status().commit();
        long prefix = h.node(0).epochIndex().positionAt(before).lastEpoch();
        var reads = new ArrayList<java.util.concurrent.CompletableFuture<Reply>>();
        for (var session : sessions)
            reads.add(
                    h.service(0)
                            .request(
                                    new ObserverFetch(
                                            session,
                                            h.node(0).status().epoch(),
                                            before,
                                            prefix,
                                            4096,
                                            100),
                                    h.now() + 2_000_000_000L));
        h.settle();
        assertTrue(reads.stream().noneMatch(java.util.concurrent.CompletableFuture::isDone));
        h.appendBarrier(0);
        h.settle();
        assertTrue(h.node(0).status().commit() > before);
        assertTrue(reads.stream().allMatch(f -> f.isDone() && !f.isCompletedExceptionally()));
    }

    @Test
    void wrongCommittedPrefixFailsWithoutTruncationAndRetainedPrefixReturnsSnapshot() {
        var h = leader();
        var s = register(h);
        long end = h.node(0).status().commit();
        var wrong =
                h.service(0)
                        .request(
                                new ObserverFetch(s, h.node(0).status().epoch(), end, 999, 4096, 0),
                                h.now() + 2_000_000_000L);
        h.settle();
        assertTrue(wrong.isCompletedExceptionally());
        h.compact(0);
        var fetch =
                h.service(0)
                        .request(
                                new ObserverFetch(s, h.node(0).status().epoch(), 0, 0, 4096, 0),
                                h.now() + 2_000_000_000L);
        h.settle();
        var id = ((SnapshotRequired) ((ObserverFetchReply) fetch.join()).payload()).id();
        assertEquals(end, id.endOffset());
        var chunk =
                h.service(0)
                        .request(
                                new ObserverSnapshot(s, h.node(0).status().epoch(), id, 0, 128),
                                h.now() + 2_000_000_000L);
        h.settle();
        assertEquals(128, ((ObserverSnapshotReply) chunk.join()).chunkLength());
    }

    @Test
    void staleDiskCompletionCannotReplySuccessfullyAfterLeaderChange() {
        var h = leader();
        var s = register(h);
        h.pauseDisk(0);
        var fetch =
                h.service(0)
                        .request(
                                new ObserverFetch(s, h.node(0).status().epoch(), 0, 0, 4096, 0),
                                h.now() + 2_000_000_000L);
        h.settle();
        assertFalse(fetch.isDone());
        var identity = vn.huyqt.logbroker.controller.support.ControllerTestSupport.identity(1);
        h.request(
                0,
                new Frame(
                        (short) 102,
                        false,
                        identity.clusterId(),
                        1,
                        999,
                        identity.voterHash(),
                        new BeginQuorumEpoch(h.node(0).status().epoch() + 1)));
        h.settle();
        assertTrue(fetch.isCompletedExceptionally());
        h.resumeDisk(0);
        h.settle();
        assertFalse(h.node(0).status().ready());
    }

    private static QuorumHarness leader() {
        var h = QuorumHarness.threeNodes(7, (short) 2);
        h.tickNode(0, Duration.ofSeconds(4));
        h.settle();
        return h;
    }

    private static Session register(QuorumHarness h) {
        var f =
                h.service(0)
                        .request(
                                new Register(
                                        1,
                                        new UUID(0, 1),
                                        new UUID(0, 2),
                                        -1,
                                        new Endpoint("localhost", 9092),
                                        (short) 2,
                                        (short) 2,
                                        2000),
                                h.now() + 2_000_000_000L);
        h.settle();
        return ((RegisterReply) f.join()).session();
    }

    @Test
    void fencedObserverReceivesCommittedPrefixWithoutVoting() {
        var h = leader();
        var s = register(h);
        h.pauseDisk(1);
        h.pauseDisk(2);
        h.appendBarrier(0);
        h.settle();
        long commit = h.node(0).status().commit();
        assertTrue(h.node(0).status().logEnd() > commit);
        var f =
                h.service(0)
                        .request(
                                new ObserverFetch(s, h.node(0).status().epoch(), 0, 0, 4096, 0),
                                h.now() + 2_000_000_000L);
        h.settle();
        var reply = (ObserverFetchReply) f.join();
        assertEquals(commit, reply.commitOffset());
        assertTrue(
                ((FetchData) reply.payload())
                        .batches().stream().allMatch(b -> b.nextOffset() <= commit));
        assertEquals(commit, h.node(0).status().commit());
    }

    @Test
    void secondOutstandingFetchIsRefusedAndLongPollHasFixedDeadline() {
        var h = leader();
        var s = register(h);
        var status = h.node(0).status();
        var request =
                new ObserverFetch(
                        s,
                        status.epoch(),
                        status.commit(),
                        h.node(0).epochIndex().positionAt(status.commit()).lastEpoch(),
                        4096,
                        100);
        var first = h.service(0).request(request, h.now() + 2_000_000_000L);
        h.settle();
        assertFalse(first.isDone());
        var duplicate = h.service(0).request(request, h.now() + 2_000_000_000L);
        h.settle();
        assertTrue(duplicate.isCompletedExceptionally());
        h.tick(Duration.ofMillis(100));
        h.settle();
        assertTrue(((FetchData) ((ObserverFetchReply) first.join()).payload()).batches().isEmpty());
    }
}

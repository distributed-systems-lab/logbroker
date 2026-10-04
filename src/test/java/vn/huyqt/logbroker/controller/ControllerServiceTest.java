package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.support.QuorumHarness;

import java.time.Duration;
import java.util.concurrent.CompletionException;

class ControllerServiceTest {
    @Test
    void pendingLimitOfOneRejectsSecondInvocationBeforeAppend() {
        var h = QuorumHarness.threeNodes(1, 1);
        h.elect(0);
        h.pauseDisk(1);
        h.pauseDisk(2);
        var first = h.service(0).createTopic("first", 1, h.now() + 30_000_000_000L);
        var second = h.service(0).createTopic("second", 1, h.now() + 30_000_000_000L);
        h.settle();
        assertTrue(second.isCompletedExceptionally());
        assertFalse(first.isDone());
        assertEquals(2, h.node(0).status().logEnd());
    }

    @Test
    void newLeaderReplaysTopicBeforeAdmittingConflictingCreate() {
        var h = QuorumHarness.threeNodes(1);
        h.elect(0);
        var create = h.service(0).createTopic("orders", 3, h.now() + 30_000_000_000L);
        h.settle();
        assertNotNull(create.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join());
        h.crash(0);
        h.elect(1);
        var conflict = h.service(1).createTopic("orders", 2, h.now() + 30_000_000_000L);
        h.settle();
        assertTrue(conflict.isCompletedExceptionally());
        var local = h.service(1).readLocalMetadata();
        h.settle();
        assertEquals(create.join(), local.join().topics().getFirst().id());
    }

    @Test
    void recoveredLocalViewIncludesPreviouslyCommittedTopicsBeforeNewElection() {
        var h = QuorumHarness.threeNodes(1);
        h.elect(0);
        var create = h.service(0).createTopic("recovered", 1, h.now() + 30_000_000_000L);
        h.settle();
        assertNotNull(create.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join());
        h.crash(0);
        h.restart(0);
        var local = h.service(0).readLocalMetadata();
        h.settle();
        assertEquals("recovered", local.join().topics().getFirst().name());
    }

    @Test
    void pendingSlotRemainsReservedWhileUserCallbackBlocksOffLoop() throws Exception {
        var release = new java.util.concurrent.CountDownLatch(1);
        var started = new java.util.concurrent.CountDownLatch(1);
        var events =
                new java.util.concurrent.LinkedBlockingQueue<
                        vn.huyqt.logbroker.controller.consensus.QuorumEvent>();
        try (var service = new ControllerService(1, events::offer, () -> 0)) {
            var call = service.describe();
            var blocked =
                    call.thenRun(
                            () -> {
                                started.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            long id =
                    ((vn.huyqt.logbroker.controller.consensus.QuorumEvent.Admin) events.take())
                            .invocationId();
            var status = QuorumHarness.threeNodes(1).node(0).status();
            service.complete(
                    id,
                    new vn.huyqt.logbroker.controller.protocol.QuorumProtocol.DescribeQuorumReply(
                            new vn.huyqt.logbroker.controller.protocol.QuorumProtocol.ReplyMeta(
                                    vn.huyqt.logbroker.controller.protocol.QuorumError.NONE,
                                    "",
                                    0,
                                    -1),
                            status));
            assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(service.describe().isCompletedExceptionally());
            release.countDown();
            blocked.get(2, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    void duplicatePendingCreatesShareIdentityAndConflictsFail() {
        var h = QuorumHarness.threeNodes(1);
        h.elect(0);
        long deadline = h.now() + Duration.ofSeconds(30).toNanos();
        var first = h.service(0).createTopic("orders", 3, deadline);
        var duplicate = h.service(0).createTopic("orders", 3, deadline);
        var conflict = h.service(0).createTopic("orders", 2, deadline);
        assertFalse(first.isDone());
        h.settle();
        assertEquals(
                first.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join(),
                duplicate.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join());
        assertThrows(CompletionException.class, conflict::join);
        var retry = h.service(0).createTopic("orders", 3, deadline);
        assertFalse(retry.isDone());
        h.settle();
        assertEquals(
                first.join(), retry.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join());
    }

    @Test
    void timeoutDoesNotCancelAdmittedTopicAndRetryFindsSameId() {
        var h = QuorumHarness.threeNodes(1);
        h.elect(0);
        h.pauseDisk(1);
        h.pauseDisk(2);
        var create =
                h.service(0).createTopic("events", 1, h.now() + Duration.ofMillis(50).toNanos());
        h.settle();
        h.tick(Duration.ofMillis(60));
        h.settle();
        assertThrows(CompletionException.class, create::join);
        h.resumeDisk(1);
        h.resumeDisk(2);
        h.settle();
        h.tick(Duration.ofMillis(150));
        h.settle();
        var retry =
                h.service(0).createTopic("events", 1, h.now() + Duration.ofSeconds(30).toNanos());
        h.settle();
        h.tick(Duration.ofMillis(100));
        h.settle();
        var local = h.service(0).readLocalMetadata();
        h.settle();
        assertEquals(
                retry.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS).join(),
                local.orTimeout(2, java.util.concurrent.TimeUnit.SECONDS)
                        .join()
                        .topics()
                        .getFirst()
                        .id());
    }

    @Test
    void localViewIsExplicitAndNonleaderCannotAdmitCommands() {
        var h = QuorumHarness.threeNodes(1);
        var create = h.service(0).createTopic("orders", 1, h.now() + 1_000_000_000L);
        h.settle();
        assertThrows(CompletionException.class, create::join);
        var read = h.service(0).readLocalMetadata();
        h.settle();
        assertEquals(
                vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Consistency.LOCAL,
                read.join().consistency());
        assertEquals(0, read.join().applied());
    }
}

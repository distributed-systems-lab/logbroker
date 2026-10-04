package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.support.*;
import vn.huyqt.logbroker.storage.LogConfig;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

class QuorumAdmissionTest {
    @Test
    void configMustCoverReadAndEncodingOwnership() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ControllerConfig.builder(ControllerTestSupport.identity(0))
                                .outboundBytes(16L * 1024 * 1024)
                                .build());
    }

    @Test
    void configMustFitLongestIndividualMetadataRecord() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ControllerConfig.builder(ControllerTestSupport.identity(0))
                                .logConfig(new LogConfig(512, 335, 64))
                                .build());
    }

    @Test
    void smallBatchesSplitCoalescedCreatesAndKeepBarrierAfterCohort() {
        var h = QuorumHarness.threeNodes(1, 1024, new LogConfig(512, 336, 64));
        h.elect(0);
        var calls = new ArrayList<CompletableFuture<UUID>>();
        for (int i = 0; i < 4; i++)
            calls.add(h.service(0).createTopic("x".repeat(248) + i, 1, h.now() + 30_000_000_000L));
        var read = h.service(0).readMetadata(h.now() + 30_000_000_000L);
        h.settle();
        for (int i = 0; i < 10 && !read.isDone(); i++) {
            h.tick(Duration.ofMillis(100));
            h.settle();
        }
        assertEquals(4, read.join().topics().size());
        for (var call : calls) assertNotNull(call.join());
        for (var batch : h.disk(0).batches()) {
            long size = 30;
            for (var entry : batch.entries()) size += 20 + QuorumEntryCodec.encode(entry).length;
            assertTrue(size <= 336, "Oversized storage batch " + size);
        }
    }

    @Test
    void delayedDiskRejectsExcessAdminsWithoutFailingParticipation() {
        var h = QuorumHarness.threeNodes(1);
        h.elect(0);
        h.pauseDisk(0);
        var calls = new ArrayList<CompletableFuture<?>>();
        for (int i = 0; i < 500; i++) {
            calls.add(h.service(0).readMetadata(h.now() + 30_000_000_000L));
            h.settle();
        }
        assertTrue(h.disk(0).pending() < 240);
        assertTrue(calls.stream().anyMatch(CompletableFuture::isCompletedExceptionally));
        assertNotEquals(QuorumStatus.Role.FAILED, h.node(0).status().role());
        h.resumeDisk(0);
        h.runHealthySuffix();
        assertTrue(calls.stream().allMatch(CompletableFuture::isDone));
        h.assertSafety();
    }

    @Test
    void laggingFollowerDoesNotFanOutMoreDiskJobsThanCapacity() {
        var h = QuorumHarness.threeNodes(42);
        h.elect(0);
        h.tick(Duration.ofMillis(200));
        h.settle();
        h.pauseDisk(1);
        h.isolate(2);
        for (int i = 0; i < 600; i++) {
            h.appendBarrier(0);
            h.settle();
        }
        h.resumeDisk(1);
        h.settle();
        for (int i = 0; i < 10; i++) {
            h.tick(Duration.ofMillis(100));
            h.settle();
        }
        assertEquals(h.disk(0).index().end(), h.disk(1).index().end());
        assertNotEquals(QuorumStatus.Role.FAILED, h.node(1).status().role());
        h.assertSafety();
    }
}

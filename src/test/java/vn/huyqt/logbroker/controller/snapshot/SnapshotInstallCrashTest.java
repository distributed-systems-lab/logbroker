package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.support.*;
import vn.huyqt.logbroker.storage.LogConfig;

import java.nio.file.*;
import java.util.*;

class SnapshotInstallCrashTest {
    @TempDir Path root;

    @Test
    void installationPreservesGlobalVoteAndDiscardsSpeculativeSuffix() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        UUID old;
        try (var state = QuorumStateStore.open(root, identity, io);
                var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
            old = generation.generation();
            state.persistVote(4, 1);
            generation.log().append(2, List.of(new QuorumEntry.ReadBarrier(2)));
            generation.log().flush();
            generation.checkpointCommit(1);
            var snapshots = new SnapshotStore(root, identity, io, state, 65536);
            var image = new MetadataImage(9, List.of());
            var id = snapshots.create(image, 3);
            generation.install(id, image);
            assertNotEquals(old, generation.generation());
            assertEquals(9, generation.log().start());
            assertEquals(9, generation.log().end());
            assertEquals(4, state.epoch());
            assertEquals(1, state.votedFor());
            io.failAfter(1);
            assertFalse(generation.releaseObsoleteGenerations());
            io.clearFailure();
            assertEquals(9, generation.log().end());
        }
        try (var state = QuorumStateStore.open(root, identity, io);
                var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
            assertEquals(9, generation.recoveredImage().appliedOffset());
            assertEquals(4, state.epoch());
            assertEquals(1, state.votedFor());
        }
    }

    @Test
    void eachPublicationFailureReopensOneCompleteGeneration() throws Exception {
        for (int failure = 1; failure <= 8; failure++) {
            var directory = root.resolve("case-" + failure);
            var io = new FaultFiles();
            var identity = ControllerTestSupport.identity(0);
            QuorumStateStore.format(directory, identity, io);
            try (var state = QuorumStateStore.open(directory, identity, io);
                    var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
                generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
                generation.log().flush();
                generation.checkpointCommit(1);
                var snapshots = new SnapshotStore(directory, identity, io, state, 65536);
                // Exercise the same crash points with a v2 feature image and a v1 speculative log.
                var image = new MetadataImage(9, List.of(), (short) 2, Map.of(), Map.of());
                var id = snapshots.create(image, 2);
                io.failAfter(failure);
                assertThrows(java.io.IOException.class, () -> generation.install(id, image));
                io.clearFailure();
            }
            io.powerLoss();
            try (var state = QuorumStateStore.open(directory, identity, io);
                    var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
                assertTrue(generation.committedOffset() == 1 || generation.committedOffset() == 9);
                assertEquals(
                        generation.committedOffset(), generation.recoveredImage().appliedOffset());
                if (generation.committedOffset() == 9)
                    assertEquals((short) 2, generation.recoveredImage().metadataVersion());
            }
        }
    }

    @Test
    void cancelledFenceAfterOldLogCloseReopensOldGenerationForFurtherWork() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        try (var state = QuorumStateStore.open(root, identity, io);
                var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
            var snapshots = new SnapshotStore(root, identity, io, state, 65536);
            var image = new MetadataImage(9, List.of());
            var id = snapshots.create(image, 1);
            var checks = new java.util.concurrent.atomic.AtomicInteger();
            assertThrows(
                    java.io.IOException.class,
                    () -> generation.install(id, image, () -> checks.incrementAndGet() == 1));
            generation.log().append(2, List.of(new QuorumEntry.ReadBarrier(2)));
            assertEquals(1, generation.log().end());
        }
    }
}

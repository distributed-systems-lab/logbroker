package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.support.*;
import vn.huyqt.logbroker.storage.LogConfig;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

class ControllerRecoveryTest {
    @Test
    void snapshotBeyondCoverageFailsBeforeAnyRuntimeThreadStarts() throws Exception {
        var files = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, files);
        try (var state = QuorumStateStore.open(root, identity, files)) {
            new vn.huyqt.logbroker.controller.snapshot.SnapshotStore(
                            root, identity, files, state, 65536)
                    .create(
                            new vn.huyqt.logbroker.controller.metadata.MetadataImage(9, List.of()),
                            1);
        }
        long before = runtimeThreads();
        assertThrows(
                IOException.class,
                () -> ControllerNode.open(root, ControllerConfig.defaults(identity), files));
        assertEquals(before, runtimeThreads());
    }

    private long runtimeThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(
                        t ->
                                t.isAlive()
                                        && (t.getName().equals("controller-loop")
                                                || t.getName().equals("controller-disk")))
                .count();
    }

    @TempDir Path root;

    @Test
    void checkpointLagDoesNotExposeSpeculativeTopicAndLogEpochFencesBeforeListening()
            throws Exception {
        var files = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, files);
        try (var state = QuorumStateStore.open(root, identity, files);
                var generation = GenerationStore.open(state, files, LogConfig.defaults())) {
            generation
                    .log()
                    .append(
                            1,
                            List.of(
                                    new QuorumEntry.Topic(
                                            1, new TopicCreated(new UUID(0, 1), "committed", 1))));
            generation.log().flush();
            generation.checkpointCommit(1);
            generation
                    .log()
                    .append(
                            7,
                            List.of(
                                    new QuorumEntry.Topic(
                                            7,
                                            new TopicCreated(new UUID(0, 2), "speculative", 1))));
            generation.log().flush();
        }
        try (var node = ControllerNode.open(root, ControllerConfig.defaults(identity), files)) {
            assertEquals(7, node.status().get().epoch());
            var local =
                    node.service()
                            .readLocalMetadata()
                            .get(3, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(1, local.applied());
            assertEquals(
                    List.of("committed"), local.topics().stream().map(TopicCreated::name).toList());
        }
        try (var state = QuorumStateStore.open(root, identity, files)) {
            assertEquals(7, state.epoch());
            assertEquals(-1, state.votedFor());
        }
    }

    @Test
    void invalidCommittedMetadataFailsOpenAndReleasesRootLock() throws Exception {
        var files = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, files);
        try (var state = QuorumStateStore.open(root, identity, files);
                var generation = GenerationStore.open(state, files, LogConfig.defaults())) {
            for (int i = 1; i <= 2; i++)
                generation
                        .log()
                        .append(
                                1,
                                List.of(
                                        new QuorumEntry.Topic(
                                                1,
                                                new TopicCreated(new UUID(0, i), "conflict", i))));
            generation.log().flush();
            generation.checkpointCommit(2);
        }
        assertThrows(
                IOException.class,
                () -> ControllerNode.open(root, ControllerConfig.defaults(identity), files));
        try (var state = QuorumStateStore.open(root, identity, files)) {
            assertEquals(identity, state.identity());
        }
    }

    @Test
    void missingReferencedLogAndCheckpointBeyondCoverageAreFatal() throws Exception {
        var files = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, files);
        Path data;
        try (var state = QuorumStateStore.open(root, identity, files);
                var generation = GenerationStore.open(state, files, LogConfig.defaults())) {
            data = generation.directory().resolve("00000000000000000000.log");
        }
        Files.delete(data);
        assertThrows(
                IOException.class,
                () -> ControllerNode.open(root, ControllerConfig.defaults(identity), files));
    }

    @Test
    void checkpointBeyondCoverageIsRejectedBeforeRuntimeCreation() throws Exception {
        var files = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, files);
        try (var state = QuorumStateStore.open(root, identity, files);
                var generation = GenerationStore.open(state, files, LogConfig.defaults())) {
            var id = generation.generation();
            state.journal()
                    .append(
                            StateJournal.COMMIT,
                            java.nio.ByteBuffer.allocate(24)
                                    .putLong(id.getMostSignificantBits())
                                    .putLong(id.getLeastSignificantBits())
                                    .putLong(9)
                                    .array());
        }
        assertThrows(
                IOException.class,
                () -> ControllerNode.open(root, ControllerConfig.defaults(identity), files));
    }
}

package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.storage.LogConfig;

class ObserverStoreTest {
    @TempDir Path root;

    @Test
    void failedCheckpointDoesNotPublishReceivedTailOnPowerLoss() throws Exception {
        var files = new FaultFiles();
        var cluster = new UUID(0, 1);
        ObserverStore.format(root, cluster, files, LogConfig.defaults());
        var feature =
                new QuorumBatch(
                        0,
                        List.of(
                                new QuorumEntry.FeatureLevel(
                                        1, new ClusterRecords.FeatureLevel((short) 2))));
        try (var store =
                ObserverStore.open(
                        root, cluster, files, LogConfig.defaults(), MetadataLimits.defaults())) {
            files.failAfter(1);
            assertThrows(IOException.class, () -> store.appendCommitted(List.of(feature), 1));
            assertEquals(0, store.image().appliedOffset());
            files.clearFailure();
        }
        files.powerLoss();
        try (var store =
                ObserverStore.open(
                        root, cluster, files, LogConfig.defaults(), MetadataLimits.defaults())) {
            assertEquals(0, store.image().appliedOffset());
            assertEquals(0, store.durableEnd());
            store.appendCommitted(List.of(feature), 1);
            assertEquals(1, store.image().appliedOffset());
        }
    }

    @Test
    void wholeInvalidReceivedPrefixIsRejectedBeforeAnyAppend() throws Exception {
        var files = new FaultFiles();
        var cluster = new UUID(0, 1);
        ObserverStore.format(root, cluster, files, LogConfig.defaults());
        try (var store =
                ObserverStore.open(
                        root, cluster, files, LogConfig.defaults(), MetadataLimits.defaults())) {
            var a =
                    new QuorumBatch(
                            0,
                            List.of(
                                    new QuorumEntry.FeatureLevel(
                                            1, new ClusterRecords.FeatureLevel((short) 2))));
            var b = new QuorumBatch(1, List.of(new QuorumEntry.ReadBarrier(1)));
            assertThrows(IOException.class, () -> store.appendCommitted(List.of(a, b), 1));
            assertEquals(0, store.durableEnd());
            store.appendCommitted(List.of(a), 1);
            assertEquals(1, store.durableEnd());
        }
    }

    @Test
    void cannotPersistUncommittedBatch() throws Exception {
        var files = new FaultFiles();
        ObserverStore.format(root, new UUID(0, 1), files, LogConfig.defaults());
        try (var store =
                ObserverStore.open(
                        root,
                        new UUID(0, 1),
                        files,
                        LogConfig.defaults(),
                        MetadataLimits.defaults())) {
            var batch = new QuorumBatch(0, List.of(new QuorumEntry.ReadBarrier(1)));
            assertThrows(IOException.class, () -> store.appendCommitted(List.of(batch), 0));
            assertEquals(0, store.durableEnd());
            assertEquals(0, store.image().appliedOffset());
        }
    }

    @Test
    void committedFeatureReplaysAndMembershipIsPinnedAcrossRestart() throws Exception {
        var files = new FaultFiles();
        var cluster = new UUID(0, 1);
        ObserverStore.format(root, cluster, files, LogConfig.defaults());
        var feature =
                new QuorumBatch(
                        0,
                        List.of(
                                new QuorumEntry.LeaderChange(1, 0),
                                new QuorumEntry.FeatureLevel(
                                        1, new ClusterRecords.FeatureLevel((short) 2))));
        byte[] hash = new byte[32];
        hash[0] = 1;
        UUID generation;
        try (var store =
                ObserverStore.open(
                        root, cluster, files, LogConfig.defaults(), MetadataLimits.defaults())) {
            store.pinMembership(hash);
            store.appendCommitted(List.of(feature), 2);
            generation = store.generation();
            assertEquals(2, store.durableEnd());
            assertEquals((short) 2, store.image().metadataVersion());
        }
        try (var store =
                ObserverStore.open(
                        root, cluster, files, LogConfig.defaults(), MetadataLimits.defaults())) {
            assertEquals(generation, store.generation());
            assertEquals(2, store.image().appliedOffset());
            byte[] foreign = hash.clone();
            foreign[0] = 2;
            assertThrows(IOException.class, () -> store.pinMembership(foreign));
        }
        assertFalse(Files.exists(root.resolve("quorum-state.journal")));
    }
}

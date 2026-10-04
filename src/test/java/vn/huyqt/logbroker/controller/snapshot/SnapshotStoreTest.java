package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.persistence.QuorumStateStore;
import vn.huyqt.logbroker.controller.support.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

class SnapshotStoreTest {
    @TempDir Path root;

    @Test
    void observerAndVoterUploadIdsAreSeparateAndPinsExpireOrCloseOnEpochChange() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        try (var state = QuorumStateStore.open(root, identity, io);
                var store = new SnapshotStore(root, identity, io, state, 65536)) {
            var id = store.create(new MetadataImage(2, List.of()), 1);
            store.readUpload(1, id, 0, 1);
            store.readObserverUpload(1, id, 0, 1);
            assertEquals(2, store.activeUploads());
            var refused =
                    assertThrows(
                            SnapshotStore.Unavailable.class,
                            () -> store.readObserverUpload(2, id, 0, 1));
            assertEquals(
                    vn.huyqt.logbroker.controller.protocol.QuorumError.OVERLOADED, refused.error());
            store.expireUploads(System.nanoTime(), true);
            assertEquals(1, store.activeUploads());
            store.expireUploads(System.nanoTime() + 31_000_000_000L, false);
            assertEquals(0, store.activeUploads());
        }
    }

    @Test
    void versionTwoFeatureImagePublishesAndReopens() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        var image = new MetadataImage(9, List.of(), (short) 2, Map.of(), Map.of());
        SnapshotId id;
        try (var state = QuorumStateStore.open(root, identity, io)) {
            var store = new SnapshotStore(root, identity, io, state, 65536);
            id = store.create(image, 3);
            byte[] bytes =
                    Files.readAllBytes(
                            root.resolve("snapshots").resolve(id.contentId() + ".snapshot"));
            assertEquals(2, java.nio.ByteBuffer.wrap(bytes).getShort(4));
            assertEquals(image, store.load(id));
        }
        try (var state = QuorumStateStore.open(root, identity, io)) {
            assertEquals(image, new SnapshotStore(root, identity, io, state, 65536).load(id));
        }
    }

    @Test
    void largeVersionTwoImageUsesBoundedChunksAndPreservesEveryAssignment() throws Exception {
        var limits =
                new vn.huyqt.logbroker.controller.metadata.MetadataLimits(
                        32, 1024, 8192, 4 * 1024 * 1024);
        var session =
                new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session(
                        1, new UUID(0, 1), new UUID(0, 2), 2);
        var registration =
                new vn.huyqt.logbroker.controller.metadata.ClusterRecords.BrokerRegistration(
                        session,
                        new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint(
                                "localhost", 9092),
                        (short) 2,
                        (short) 2);
        var topics = new ArrayList<TopicCreated>();
        var partitions =
                new TreeMap<
                        MetadataImage.PartitionKey,
                        vn.huyqt.logbroker.controller.metadata.ClusterRecords.PartitionRecord>();
        for (int i = 1; i <= 1024; i++) {
            var id = new UUID(0, i);
            topics.add(new TopicCreated(id, "t" + i + "x".repeat(240), 8));
            for (int p = 0; p < 8; p++)
                partitions.put(
                        new MetadataImage.PartitionKey(id, p),
                        new vn.huyqt.logbroker.controller.metadata.ClusterRecords.PartitionRecord(
                                id, p, List.of(1), 1, 0, 0));
        }
        var image =
                new MetadataImage(
                        10000,
                        topics,
                        (short) 2,
                        Map.of(1, new MetadataImage.BrokerRegistrationView(registration, false, 3)),
                        partitions);
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        SnapshotId id;
        try (var state = QuorumStateStore.open(root, identity, io)) {
            var store = new SnapshotStore(root, identity, io, state, 4 * 1024 * 1024, limits);
            id = store.create(image, 3);
            var bytes = new java.io.ByteArrayOutputStream();
            try (var pin = store.pin(id)) {
                assertTrue(pin.length() > 256 * 1024);
                for (long position = 0; position < pin.length(); position += 65536)
                    bytes.write(pin.read(position, 65536));
            }
            assertEquals(image, store.decode(id, bytes.toByteArray()));
        }
        try (var state = QuorumStateStore.open(root, identity, io)) {
            assertEquals(
                    image,
                    new SnapshotStore(root, identity, io, state, 4 * 1024 * 1024, limits).load(id));
        }
    }

    @Test
    void publishedSnapshotReopensAndPinnedChunksMatchCompleteFile() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        SnapshotId id;
        var image = new MetadataImage(9, List.of(new TopicCreated(new UUID(0, 9), "orders", 3)));
        try (var state = QuorumStateStore.open(root, identity, io)) {
            var store = new SnapshotStore(root, identity, io, state, 65536);
            id = store.create(image, 3);
            assertEquals(image, store.load(id));
            try (var pin = store.pin(id)) {
                byte[] all =
                        Files.readAllBytes(
                                root.resolve("snapshots").resolve(id.contentId() + ".snapshot"));
                assertEquals(all.length, pin.length());
                assertArrayEquals(Arrays.copyOfRange(all, 0, 32), pin.read(0, 32));
                assertThrows(IllegalArgumentException.class, () -> pin.read(-1, 32));
            }
        }
        try (var state = QuorumStateStore.open(root, identity, io)) {
            var store = new SnapshotStore(root, identity, io, state, 65536);
            assertEquals(List.of(id), store.retained());
            assertEquals(image, store.load(id));
        }
    }

    @Test
    void publishedCorruptionIsFatalAndPartialFilesNeverBecomeSnapshots() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        try (var state = QuorumStateStore.open(root, identity, io)) {
            var store = new SnapshotStore(root, identity, io, state, 65536);
            SnapshotId id = store.create(new MetadataImage(2, List.of()), 1);
            Files.write(root.resolve("snapshots/orphan.partial"), new byte[] {1});
            byte[] bytes =
                    Files.readAllBytes(
                            root.resolve("snapshots").resolve(id.contentId() + ".snapshot"));
            bytes[bytes.length - 1] ^= 1;
            Files.write(root.resolve("snapshots").resolve(id.contentId() + ".snapshot"), bytes);
            assertThrows(IOException.class, () -> store.load(id));
            assertThrows(
                    IOException.class, () -> new SnapshotStore(root, identity, io, state, 65536));
        }
    }

    @Test
    void imageBoundaryAndClusterIdentityMustMatchSnapshot() throws Exception {
        var io = new FaultFiles();
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, io);
        try (var state = QuorumStateStore.open(root, identity, io)) {
            var store = new SnapshotStore(root, identity, io, state, 65536);
            var id = new SnapshotId(9, 3, new UUID(0, 9));
            assertThrows(
                    IOException.class, () -> store.encode(id, new MetadataImage(8, List.of())));
            byte[] wrong = store.encode(id, new MetadataImage(9, List.of()));
            java.nio.ByteBuffer.wrap(wrong).putLong(22, 99); // cluster UUID low half
            assertThrows(IOException.class, () -> store.decode(id, wrong));
        }
    }
}

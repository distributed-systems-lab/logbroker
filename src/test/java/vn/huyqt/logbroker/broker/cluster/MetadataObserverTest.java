package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.SnapshotRequired;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

class MetadataObserverTest {
    @TempDir Path root;

    @Test
    void observerDoesNotFetchAheadOfDurableCompletion() throws Exception {
        try (var h = new ObserverHarness(root)) {
            h.start();
            h.completeFetchWithOneCommittedBatch(1);
            assertEquals(1, h.fetches());
            assertEquals(0, h.observer.image().appliedOffset());
            h.runOneDiskTask();
            assertEquals(2, h.fetches());
            assertEquals(1, h.observer.image().appliedOffset());
            assertEquals(1, h.published.size());
            assertTrue(h.fatal.isEmpty());
        }
    }

    @Test
    void uncommittedWireReplyIsRejectedBeforeDiskAppend() throws Exception {
        try (var h = new ObserverHarness(root)) {
            h.start();
            h.completeFetchWithOneCommittedBatch(0);
            assertEquals(0, h.store.durableEnd());
            assertTrue(h.published.isEmpty());
            assertTrue(h.disk.isEmpty());
            assertEquals(1, h.fetches());
        }
    }

    @Test
    void stopDrainsQueuedDiskAndSuppressesPublication() throws Exception {
        try (var h = new ObserverHarness(root)) {
            h.start();
            h.completeFetchWithOneCommittedBatch(1);
            var stopped = h.observer.stop();
            h.clock.runDue();
            assertFalse(stopped.isDone());
            h.runOneDiskTask();
            while (!h.disk.isEmpty()) h.runOneDiskTask();
            assertTrue(stopped.isDone());
            assertTrue(h.published.isEmpty());
            assertEquals(0, h.store.durableEnd());
        }
    }

    @Test
    void failedCheckpointStopsPullLoop() throws Exception {
        try (var h = new ObserverHarness(root)) {
            h.start();
            h.completeFetchWithOneCommittedBatch(1);
            h.files.failAfter(1);
            h.runOneDiskTask();
            h.files.clearFailure();
            assertFalse(h.fatal.isEmpty());
            assertEquals(1, h.fetches());
            assertEquals(0, h.observer.image().appliedOffset());
        }
    }

    @Test
    void snapshotCatchupPublishesOnlyAfterGenerationForce() throws Exception {
        try (var h = new ObserverHarness(root)) {
            h.start();
            UUID old = h.store.generation();
            var id = new SnapshotId(9, 1, new UUID(0, 99));
            var restored = new MetadataImage(9, List.of(), (short) 2, Map.of(), Map.of());
            byte[] bytes = h.store.snapshots().encode(id, restored);
            h.transport
                    .last()
                    .replyNext(
                            new BrokerControlProtocol.ObserverFetchReply(
                                    BrokerControlClientTest.describe().meta(),
                                    9,
                                    new SnapshotRequired(id)));
            h.clock.runDue();
            h.runOneDiskTask();
            h.transport
                    .last()
                    .replyNext(
                            new BrokerControlProtocol.ObserverSnapshotReply(
                                    BrokerControlClientTest.describe().meta(),
                                    id,
                                    0,
                                    bytes.length,
                                    bytes));
            h.clock.runDue();
            assertEquals(0, h.observer.image().appliedOffset());
            h.runOneDiskTask();
            assertEquals(old, h.store.generation());
            assertEquals(0, h.observer.image().appliedOffset());
            h.runOneDiskTask();
            assertEquals(9, h.observer.image().appliedOffset());
            assertNotEquals(old, h.store.generation());
            assertEquals(2, h.fetches());
            assertTrue(h.fatal.isEmpty());
        }
    }

    @Test
    void corruptSnapshotContentCancelsTransferAndRetriesWithoutPublishing() throws Exception {
        try (var h = new ObserverHarness(root)) {
            h.start();
            var id = new SnapshotId(9, 1, new UUID(0, 99));
            byte[] bytes =
                    h.store
                            .snapshots()
                            .encode(
                                    id,
                                    new MetadataImage(9, List.of(), (short) 2, Map.of(), Map.of()));
            bytes[bytes.length - 1] ^= 1;
            h.transport
                    .last()
                    .replyNext(
                            new BrokerControlProtocol.ObserverFetchReply(
                                    BrokerControlClientTest.describe().meta(),
                                    9,
                                    new SnapshotRequired(id)));
            h.clock.runDue();
            h.runOneDiskTask();
            h.transport
                    .last()
                    .replyNext(
                            new BrokerControlProtocol.ObserverSnapshotReply(
                                    BrokerControlClientTest.describe().meta(),
                                    id,
                                    0,
                                    bytes.length,
                                    bytes));
            h.clock.runDue();
            h.runOneDiskTask();
            h.runOneDiskTask();
            assertTrue(h.fatal.isEmpty());
            assertTrue(h.published.isEmpty());
            assertEquals(0, h.store.durableEnd());
            h.runOneDiskTask();
            assertEquals(0, h.store.snapshots().downloadedBytes());
        }
    }
}

package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.support.ManualScheduler;

class ClusterPartitionManagerTest {
    @TempDir Path root;
    static final UUID TOPIC = new UUID(0, 7), STORAGE = new UUID(0, 9);
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    static Session session() {
        return new Session(1, STORAGE, new UUID(0, 2), 2);
    }

    static MetadataImage image() {
        var registration =
                new BrokerRegistration(
                        session(), new Endpoint("localhost", 19092), (short) 2, (short) 2);
        return new MetadataImage(
                4,
                List.of(new TopicCreated(TOPIC, "orders", 1)),
                (short) 2,
                Map.of(1, new MetadataImage.BrokerRegistrationView(registration, false, 3)),
                Map.of(
                        new MetadataImage.PartitionKey(TOPIC, 0),
                        new PartitionRecord(TOPIC, 0, List.of(1), 1, 1, 1)));
    }

    @Test
    void forcedCompletionPrecedesRuntimePublicationAndReopen() throws Exception {
        var files = new FaultFiles();
        PartitionInventory.format(root, STORAGE, files);
        Files.createDirectory(root.resolve("partitions"));
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 8, 8)) {
            var inventory = PartitionInventory.open(root, STORAGE, files);
            var disk = new ArrayDeque<Runnable>();
            var manager =
                    new ClusterPartitionManager(
                            inventory,
                            FilePartitionStore.clusterFactory(),
                            disk::add,
                            root,
                            new ServingGate(session()),
                            files,
                            BrokerConfig.defaults(root),
                            lanes,
                            clock);
            var reconciled = manager.reconcile(image(), session());
            assertTrue(manager.runtime(TP).isEmpty());
            assertFalse(reconciled.isDone());
            disk.removeFirst().run();
            assertTrue(reconciled.isDone());
            assertTrue(manager.runtime(TP).isPresent(), manager.failures().toString());
            assertEquals(PartitionInventory.State.COMPLETE, inventory.state(TP));
            var close = manager.closeAsync();
            disk.removeFirst().run();
            close.get(2, TimeUnit.SECONDS);
            try (var reopened = PartitionInventory.open(root, STORAGE, files)) {
                assertEquals(PartitionInventory.State.COMPLETE, reopened.state(TP));
            }
        }
    }

    @Test
    void missingCompleteLogNeverCallsCreatingFactory() throws Exception {
        var files = new FaultFiles();
        PartitionInventory.format(root, STORAGE, files);
        Files.createDirectory(root.resolve("partitions"));
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 8, 8)) {
            var inventory = PartitionInventory.open(root, STORAGE, files);
            inventory.begin(TP);
            inventory.complete(TP);
            var disk = new ArrayDeque<Runnable>();
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var manager =
                    new ClusterPartitionManager(
                            inventory,
                            (path, config) -> {
                                calls.incrementAndGet();
                                return FilePartitionStore.open(path, config);
                            },
                            disk::add,
                            root,
                            new ServingGate(session()),
                            files,
                            BrokerConfig.defaults(root),
                            lanes,
                            clock);
            manager.reconcile(image(), session());
            disk.removeFirst().run();
            assertTrue(manager.runtime(TP).isEmpty());
            assertTrue(manager.failures().containsKey(TP));
            assertEquals(0, calls.get());
            assertFalse(
                    Files.exists(
                            root.resolve("partitions").resolve(TOPIC.toString()).resolve("0")));
            var close = manager.closeAsync();
            disk.removeFirst().run();
            close.get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void supersededOpenAndShutdownCannotPublishRuntime() throws Exception {
        var files = new FaultFiles();
        PartitionInventory.format(root, STORAGE, files);
        Files.createDirectory(root.resolve("partitions"));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var disk = Executors.newSingleThreadExecutor();
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 8, 8)) {
            var inventory = PartitionInventory.open(root, STORAGE, files);
            PartitionStore.Factory factory =
                    (path, config) -> {
                        entered.countDown();
                        try {
                            if (!release.await(3, TimeUnit.SECONDS))
                                throw new java.io.IOException("Open test timed out");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new java.io.IOException(e);
                        }
                        return FilePartitionStore.open(path, config);
                    };
            var manager =
                    new ClusterPartitionManager(
                            inventory,
                            factory,
                            disk,
                            root,
                            new ServingGate(session()),
                            files,
                            BrokerConfig.defaults(root),
                            lanes,
                            clock);
            manager.reconcile(image(), session());
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var stopped = manager.closeAsync();
            assertFalse(stopped.isDone());
            assertTrue(manager.runtime(TP).isEmpty());
            release.countDown();
            stopped.get(3, TimeUnit.SECONDS);
            assertTrue(manager.runtime(TP).isEmpty());
            // The obsolete store was closed, so strict reopen can acquire its partition lock.
            try (var reopened =
                    FilePartitionStore.open(
                            root.resolve("partitions").resolve(TOPIC.toString()).resolve("0"),
                            BrokerConfig.defaults(root).logConfig())) {}
        } finally {
            release.countDown();
            disk.shutdownNow();
            assertTrue(disk.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    void reconciliationBacklogCoalescesAndOneFailedOpenDoesNotBlockOtherPartitions()
            throws Exception {
        var files = new FaultFiles();
        PartitionInventory.format(root, STORAGE, files);
        Files.createDirectory(root.resolve("partitions"));
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 8, 8)) {
            var inventory = PartitionInventory.open(root, STORAGE, files);
            var disk = new ArrayDeque<Runnable>();
            PartitionStore.Factory factory =
                    (path, config) -> {
                        if (path.getFileName().toString().equals("0"))
                            throw new java.io.IOException("Broken partition 0");
                        return FilePartitionStore.open(path, config);
                    };
            var manager =
                    new ClusterPartitionManager(
                            inventory,
                            factory,
                            disk::add,
                            root,
                            new ServingGate(session()),
                            files,
                            BrokerConfig.defaults(root),
                            lanes,
                            clock);
            var partitions = new HashMap<>(image().partitions());
            partitions.put(
                    new MetadataImage.PartitionKey(TOPIC, 1),
                    new PartitionRecord(TOPIC, 1, List.of(1), 1, 1, 1));
            CompletableFuture<Void> first = null, last = null;
            for (int i = 0; i < 100; i++) {
                last =
                        manager.reconcile(
                                new MetadataImage(
                                        5 + i,
                                        List.of(new TopicCreated(TOPIC, "orders", 2)),
                                        (short) 2,
                                        image().brokers(),
                                        partitions),
                                session());
                if (i == 0) first = last;
            }
            assertEquals(1, disk.size());
            assertTrue(first.isCompletedExceptionally());
            disk.removeFirst().run();
            assertTrue(last.isDone());
            assertTrue(manager.runtime(TP).isEmpty());
            assertTrue(manager.runtime(new TopicPartition(TOPIC, 1)).isPresent());
            assertEquals(
                    PartitionInventory.State.COMPLETE,
                    inventory.state(new TopicPartition(TOPIC, 1)));
            var closed = manager.closeAsync();
            disk.removeFirst().run();
            closed.get(2, TimeUnit.SECONDS);
        }
    }
}

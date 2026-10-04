package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class ClusterAdmissionGateTest {
    @TempDir Path root;

    @Test
    void fencingQueuedAppendRejectsBeforeAnyMutation() throws Exception {
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var released = new CountDownLatch(1);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store =
                        FilePartitionStore.open(root, BrokerConfig.defaults(root).logConfig())) {
            var blocked = new CountDownLatch(1);
            lanes.submit(
                    tp,
                    () -> {
                        blocked.countDown();
                        released.await();
                        return null;
                    });
            assertTrue(blocked.await(2, TimeUnit.SECONDS));
            var allowed = new AtomicBoolean(true);
            var runtime =
                    new PartitionRuntime(tp, store, lanes, clock, BrokerConfig.defaults(root));
            var result =
                    runtime.produce(
                            batch(), Protocol.AckMode.APPENDED, 1_000_000_000L, allowed::get);
            allowed.set(false);
            released.countDown();
            assertEquals(
                    ClusterProtocol.Outcome.REJECTED, result.get(2, TimeUnit.SECONDS).outcome());
            assertEquals(ErrorCode.FENCED_BROKER, result.join().error().code());
            assertEquals(0, store.logEndOffset());
            runtime.closeAsync().get(2, TimeUnit.SECONDS);
        } finally {
            released.countDown();
        }
    }

    @Test
    void fencingAnAppendedFlushedWaiterHasUnknownOutcome() throws Exception {
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store =
                        FilePartitionStore.open(root, BrokerConfig.defaults(root).logConfig())) {
            var allowed = new AtomicBoolean(true);
            var runtime =
                    new PartitionRuntime(tp, store, lanes, clock, BrokerConfig.defaults(root));
            var result =
                    runtime.produce(
                            batch(), Protocol.AckMode.FLUSHED, 1_000_000_000L, allowed::get);
            lanes.drain().get(2, TimeUnit.SECONDS);
            assertFalse(result.isDone());
            allowed.set(false);
            runtime.permissionChanged();
            lanes.drain().get(2, TimeUnit.SECONDS);
            assertEquals(
                    ClusterProtocol.Outcome.UNKNOWN, result.get(2, TimeUnit.SECONDS).outcome());
            assertEquals(1, store.logEndOffset());
            runtime.closeAsync().get(2, TimeUnit.SECONDS);
        }
    }

    static Protocol.Batch batch() {
        return new Protocol.Batch(List.of(new LogRecord(0, null, new byte[] {1}, List.of())));
    }

    @Test
    void dispatcherRequiresClusterAndBothRouteEpochsBeforeAppend() throws Exception {
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var session =
                new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session(
                        3, new UUID(0, 2), new UUID(0, 3), 11);
        var registration =
                new vn.huyqt.logbroker.controller.metadata.ClusterRecords.BrokerRegistration(
                        session,
                        new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint(
                                "localhost", 9092),
                        (short) 2,
                        (short) 2);
        var image =
                new vn.huyqt.logbroker.controller.metadata.MetadataImage(
                        20,
                        List.of(
                                new vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated(
                                        tp.topicId(), "orders", 1)),
                        (short) 2,
                        java.util.Map.of(
                                3,
                                new vn.huyqt.logbroker.controller.metadata.MetadataImage
                                        .BrokerRegistrationView(registration, false, 12)),
                        java.util.Map.of(
                                new vn.huyqt.logbroker.controller.metadata.MetadataImage
                                        .PartitionKey(tp.topicId(), 0),
                                new vn.huyqt.logbroker.controller.metadata.ClusterRecords
                                        .PartitionRecord(tp.topicId(), 0, List.of(3), 3, 17, 17)));
        var metadata =
                new vn.huyqt.logbroker.broker.metadata.BrokerMetadata() {
                    public vn.huyqt.logbroker.controller.metadata.MetadataImage image() {
                        return image;
                    }

                    public CompletableFuture<CreateResult> create(
                            String name, int count, long deadline) {
                        return CompletableFuture.failedFuture(new UnsupportedOperationException());
                    }
                };
        var config = BrokerConfig.defaults(root);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store = FilePartitionStore.open(root, config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, lanes, clock, config);
            var gate = new vn.huyqt.logbroker.broker.cluster.ServingGate(session);
            gate.apply(20, 12, false);
            java.util.function.Function<Protocol.TopicPartition, PartitionRuntime> resolve =
                    key -> key.equals(tp) ? runtime : null;
            try (var fetch =
                    new FetchCoordinator(
                            new FetchPlanner(resolve, config),
                            resolve,
                            clock,
                            new ResourceBudget(8))) {
                var dispatcher =
                        new RequestDispatcher(
                                metadata, resolve, fetch, clock, new UUID(0, 1), gate);
                var stale =
                        new ClusterProtocol.Produce(
                                new UUID(0, 1),
                                Protocol.AckMode.APPENDED,
                                1000,
                                List.of(
                                        new ClusterProtocol.ProduceEntry(
                                                new ClusterProtocol.Route(tp, 3, 11, 16),
                                                batch())));
                var reply =
                        (ClusterProtocol.ProduceReply)
                                dispatcher
                                        .handle(
                                                new RequestContext(1, 1, 1_000_000_000L, (short) 2),
                                                stale)
                                        .get(2, TimeUnit.SECONDS);
                assertEquals(
                        ErrorCode.STALE_PARTITION_EPOCH, reply.results().getFirst().error().code());
                assertEquals(
                        ClusterProtocol.Outcome.REJECTED, reply.results().getFirst().outcome());
                assertEquals(0, store.logEndOffset());
                var foreign =
                        new ClusterProtocol.Produce(
                                new UUID(0, 99), stale.ack(), 1000, stale.entries());
                assertEquals(
                        ErrorCode.CLUSTER_MISMATCH,
                        ((Protocol.Failure)
                                        dispatcher
                                                .handle(
                                                        new RequestContext(
                                                                1, 2, 1_000_000_000L, (short) 2),
                                                        foreign)
                                                .join())
                                .error()
                                .code());
                var valid =
                        new ClusterProtocol.Produce(
                                new UUID(0, 1),
                                stale.ack(),
                                1000,
                                List.of(
                                        new ClusterProtocol.ProduceEntry(
                                                new ClusterProtocol.Route(tp, 3, 11, 17), batch()),
                                        new ClusterProtocol.ProduceEntry(
                                                new ClusterProtocol.Route(
                                                        new Protocol.TopicPartition(
                                                                tp.topicId(), 1),
                                                        3,
                                                        11,
                                                        17),
                                                batch())));
                var partial =
                        (ClusterProtocol.ProduceReply)
                                dispatcher
                                        .handle(
                                                new RequestContext(1, 3, 1_000_000_000L, (short) 2),
                                                valid)
                                        .get(2, TimeUnit.SECONDS);
                assertEquals(
                        ClusterProtocol.Outcome.SUCCESS, partial.results().getFirst().outcome());
                assertEquals(
                        ClusterProtocol.Outcome.REJECTED, partial.results().getLast().outcome());
                assertEquals(
                        ErrorCode.UNKNOWN_PARTITION, partial.results().getLast().error().code());
                runtime.closeAsync().get(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void deadlineDuringBlockedAppendIsUnknownAndDoesNotCancelTheMutation() throws Exception {
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var release = new CountDownLatch(1);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var backing =
                        FilePartitionStore.open(root, BrokerConfig.defaults(root).logConfig())) {
            var entered = new CountDownLatch(1);
            PartitionStore blocking =
                    new PartitionStore() {
                        public vn.huyqt.logbroker.storage.AppendResult append(
                                List<LogRecord> records) throws java.io.IOException {
                            entered.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException e) {
                                throw new java.io.IOException(e);
                            }
                            return backing.append(records);
                        }

                        public List<vn.huyqt.logbroker.storage.RecordBatch> read(
                                long offset, int max) throws java.io.IOException {
                            return backing.read(offset, max);
                        }

                        public long flush() throws java.io.IOException {
                            return backing.flush();
                        }

                        public long logStartOffset() {
                            return backing.logStartOffset();
                        }

                        public long logEndOffset() {
                            return backing.logEndOffset();
                        }

                        public long durableEndOffset() {
                            return backing.durableEndOffset();
                        }

                        public void close() {}
                    };
            var runtime =
                    new PartitionRuntime(tp, blocking, lanes, clock, BrokerConfig.defaults(root));
            var result =
                    runtime.produce(batch(), Protocol.AckMode.APPENDED, 1_000_000_000L, () -> true);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            clock.advance(java.time.Duration.ofSeconds(1));
            clock.runDue();
            assertEquals(
                    ClusterProtocol.Outcome.UNKNOWN, result.get(2, TimeUnit.SECONDS).outcome());
            release.countDown();
            lanes.drain().get(2, TimeUnit.SECONDS);
            assertEquals(1, backing.logEndOffset());
            assertEquals(ClusterProtocol.Outcome.UNKNOWN, result.join().outcome());
            runtime.closeAsync().get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Test
    void fencingCannotBeLostBehindAnAlreadyPendingFlushControlTask() throws Exception {
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var release = new CountDownLatch(1);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store =
                        FilePartitionStore.open(root, BrokerConfig.defaults(root).logConfig())) {
            var allowed = new AtomicBoolean(true);
            var runtime =
                    new PartitionRuntime(tp, store, lanes, clock, BrokerConfig.defaults(root));
            var result =
                    runtime.produce(
                            batch(), Protocol.AckMode.FLUSHED, 1_000_000_000L, allowed::get);
            lanes.drain().get(2, TimeUnit.SECONDS);
            var entered = new CountDownLatch(1);
            lanes.submit(
                    tp,
                    () -> {
                        entered.countDown();
                        release.await();
                        return null;
                    });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            runtime.requestFlush(); // Occupies the single coalescing control slot before fencing.
            allowed.set(false);
            runtime.permissionChanged();
            release.countDown();
            lanes.drain().get(2, TimeUnit.SECONDS);
            assertEquals(
                    ClusterProtocol.Outcome.UNKNOWN,
                    result.get(200, TimeUnit.MILLISECONDS).outcome());
            assertEquals(ErrorCode.FENCED_BROKER, result.join().error().code());
            runtime.closeAsync().get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }
}

package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.protocol.*;
import vn.huyqt.logbroker.support.ManualScheduler;

class ClusterFetchBoundaryTest {
    @TempDir Path root;

    @Test
    void fetchReadsOnlyTheDurablePrefixAndCapturesAllBoundariesTogether() throws Exception {
        var config = BrokerConfig.defaults(root);
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store = FilePartitionStore.open(root, config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, lanes, clock, config);
            runtime.produce(
                            ClusterAdmissionGateTest.batch(),
                            Protocol.AckMode.APPENDED,
                            1_000_000_000L,
                            () -> true)
                    .get(2, TimeUnit.SECONDS);
            var entry = new Protocol.FetchEntry(tp, 0, 4096);
            var before = runtime.read(entry, 4096, false, () -> true).get(2, TimeUnit.SECONDS);
            assertEquals(1, before.logEndOffset());
            assertEquals(0, before.highWatermark());
            assertTrue(before.batches().isEmpty());
            runtime.flushNow().get(2, TimeUnit.SECONDS);
            var after = runtime.read(entry, 4096, false, () -> true).get(2, TimeUnit.SECONDS);
            assertEquals(1, after.highWatermark());
            assertEquals(1, after.batches().size());
            var denied = runtime.read(entry, 4096, false, () -> false).get(2, TimeUnit.SECONDS);
            assertEquals(ErrorCode.FENCED_BROKER, denied.error().code());
            runtime.closeAsync().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void longPollWakesWhenTheDurablePrefixAdvances() throws Exception {
        var config = BrokerConfig.defaults(root);
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store = FilePartitionStore.open(root, config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, lanes, clock, config);
            runtime.produce(
                            ClusterAdmissionGateTest.batch(),
                            Protocol.AckMode.APPENDED,
                            1_000_000_000L,
                            () -> true)
                    .get(2, TimeUnit.SECONDS);
            java.util.function.Function<Protocol.TopicPartition, PartitionRuntime> lookup =
                    ignored -> runtime;
            try (var fetch =
                    new FetchCoordinator(
                            new FetchPlanner(lookup, config),
                            lookup,
                            clock,
                            new ResourceBudget(8))) {
                var request =
                        new ClusterProtocol.Fetch(
                                new UUID(0, 1),
                                4096,
                                1,
                                5000,
                                java.util.List.of(
                                        new ClusterProtocol.FetchEntry(
                                                new ClusterProtocol.Route(tp, 3, 11, 17),
                                                0,
                                                4096)));
                var pending =
                        fetch.fetch(
                                new RequestContext(1, 1, 10_000_000_000L, (short) 2),
                                request,
                                ignored -> ErrorCode.NONE,
                                ignored -> () -> true);
                lanes.drain().get(2, TimeUnit.SECONDS);
                assertFalse(pending.isDone());
                runtime.flushNow().get(2, TimeUnit.SECONDS);
                assertEquals(
                        1, pending.get(2, TimeUnit.SECONDS).results().getFirst().highWatermark());
                runtime.closeAsync().get(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void longPollWakesOnCommittedFencingWithoutWaitingForDeadline() throws Exception {
        var config = BrokerConfig.defaults(root);
        var tp = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var session =
                new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session(
                        3, new UUID(0, 2), new UUID(0, 3), 11);
        var gate = new vn.huyqt.logbroker.broker.cluster.ServingGate(session);
        gate.apply(12, 12, false);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(1, 1, 8);
                var store = FilePartitionStore.open(root, config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, lanes, clock, config);
            java.util.function.Function<Protocol.TopicPartition, PartitionRuntime> lookup =
                    ignored -> runtime;
            try (var fetch =
                    new FetchCoordinator(
                            new FetchPlanner(lookup, config),
                            lookup,
                            clock,
                            new ResourceBudget(8))) {
                var request =
                        new ClusterProtocol.Fetch(
                                new UUID(0, 1),
                                4096,
                                1,
                                5000,
                                java.util.List.of(
                                        new ClusterProtocol.FetchEntry(
                                                new ClusterProtocol.Route(tp, 3, 11, 17),
                                                0,
                                                4096)));
                var pending =
                        fetch.fetch(
                                new RequestContext(1, 1, 10_000_000_000L, (short) 2),
                                request,
                                ignored ->
                                        gate.canServe() ? ErrorCode.NONE : ErrorCode.FENCED_BROKER,
                                gate::onChange);
                lanes.drain().get(2, TimeUnit.SECONDS);
                assertFalse(pending.isDone());
                gate.apply(13, 13, true);
                assertEquals(
                        ErrorCode.FENCED_BROKER,
                        pending.get(2, TimeUnit.SECONDS).results().getFirst().error().code());
                assertEquals(0, clock.nanoTime());
                runtime.closeAsync().get(2, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void oversizedFirstBatchMustBeDelegatedAndCannotBeUsedTwice() throws Exception {
        var config = BrokerConfig.defaults(root);
        var a = new Protocol.TopicPartition(new UUID(0, 7), 0);
        var b = new Protocol.TopicPartition(a.topicId(), 1);
        try (var clock = new ManualScheduler();
                var lanes = new PartitionExecutor(2, 2, 8);
                var storeA = FilePartitionStore.open(root.resolve("a"), config.logConfig());
                var storeB = FilePartitionStore.open(root.resolve("b"), config.logConfig())) {
            var ra = new PartitionRuntime(a, storeA, lanes, clock, config);
            var rb = new PartitionRuntime(b, storeB, lanes, clock, config);
            ra.produce(ClusterAdmissionGateTest.batch(), Protocol.AckMode.APPENDED, 1_000_000_000L)
                    .get();
            rb.produce(ClusterAdmissionGateTest.batch(), Protocol.AckMode.APPENDED, 1_000_000_000L)
                    .get();
            ra.flushNow().get();
            rb.flushNow().get();
            var planner = new FetchPlanner(java.util.Map.of(a, ra, b, rb), config);
            var entries =
                    java.util.List.of(
                            new ClusterProtocol.FetchEntry(
                                    new ClusterProtocol.Route(a, 3, 11, 17), 0, 1),
                            new ClusterProtocol.FetchEntry(
                                    new ClusterProtocol.Route(b, 3, 11, 17), 0, 1));
            var denied =
                    planner.read(
                                    new ClusterProtocol.Fetch(new UUID(0, 1), 1, 0, 0, entries),
                                    ignored -> ErrorCode.NONE)
                            .get();
            assertTrue(denied.results().stream().allMatch(result -> result.batches().isEmpty()));
            var granted =
                    planner.read(
                                    new ClusterProtocol.Fetch(
                                            new UUID(0, 1), 1, 0, 0, entries, true),
                                    ignored -> ErrorCode.NONE)
                            .get();
            assertEquals(
                    1,
                    granted.results().stream().mapToInt(result -> result.batches().size()).sum());
            assertEquals(1, granted.results().getFirst().batches().size());
            ra.closeAsync().get();
            rb.closeAsync().get();
        }
    }
}

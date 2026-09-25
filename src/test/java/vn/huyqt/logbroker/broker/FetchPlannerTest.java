package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.ManualScheduler;

class FetchPlannerTest {
    @TempDir Path directory;

    @Test
    void onlyOneBatchMayExceedTheWholeResponseBudget() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var a = new TopicPartition(new UUID(1, 1), 0);
        var b = new TopicPartition(new UUID(1, 1), 1);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(2, 2, 8);
             var storeA = FilePartitionStore.open(directory.resolve("a"), config.logConfig());
             var storeB = FilePartitionStore.open(directory.resolve("b"), config.logConfig())) {
            var runtimeA = new PartitionRuntime(a, storeA, workers, clock, config);
            var runtimeB = new PartitionRuntime(b, storeB, workers, clock, config);
            var batch = new Batch(List.of(new LogRecord(0, null, null, List.of())));
            runtimeA.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            runtimeB.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            var planner = new FetchPlanner(Map.of(a, runtimeA, b, runtimeB), config);
            var reply = planner.read(new Fetch(1, 0, 0,
                    List.of(new FetchEntry(a, 0, 1), new FetchEntry(b, 0, 1)))).get();
            assertEquals(1, reply.results().stream().mapToInt(r -> r.batches().size()).sum());
            assertEquals(1, reply.results().getFirst().batches().size());
            assertEquals(0, reply.results().get(1).batches().size());
            runtimeA.close(); runtimeB.close();
        }
    }

    @Test
    void middleOffsetReturnsWholeBatchAndEndOffsetIsEmpty() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var a = new TopicPartition(new UUID(2, 2), 0);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(1, 1, 8);
             var store = FilePartitionStore.open(directory.resolve("a"), config.logConfig())) {
            var runtime = new PartitionRuntime(a, store, workers, clock, config);
            var batch = new Batch(List.of(new LogRecord(0, null, null, List.of()),
                    new LogRecord(1, null, null, List.of())));
            runtime.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            var planner = new FetchPlanner(Map.of(a, runtime), config);
            var middle = planner.read(new Fetch(1024, 0, 0,
                    List.of(new FetchEntry(a, 1, 1024)))).get().results().getFirst();
            assertEquals(0, middle.batches().getFirst().baseOffset());
            assertEquals(2, middle.batches().getFirst().batch().records().size());
            var end = planner.read(new Fetch(1024, 0, 0,
                    List.of(new FetchEntry(a, 2, 1024)))).get().results().getFirst();
            assertTrue(end.batches().isEmpty());
            runtime.close();
        }
    }
}

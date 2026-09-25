package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.ManualScheduler;

class LongPollTest {
    @TempDir Path directory;

    @Test
    void appendWakesLongPollWithoutHoldingPartitionWorker() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 2), 0);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(1, 1, 8);
             var store = FilePartitionStore.open(directory.resolve("partition"), config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, workers, clock, config);
            var coordinator = new FetchCoordinator(new FetchPlanner(Map.of(tp, runtime), config),
                    Map.of(tp, runtime)::get, clock, new ResourceBudget(8));
            var waiting = coordinator.fetch(new RequestContext(1, 7,
                    TimeUnit.SECONDS.toNanos(1)), new Fetch(1024, 42, 500,
                    List.of(new FetchEntry(tp, 0, 1024))));
            workers.drain().get(2, TimeUnit.SECONDS);
            assertFalse(waiting.isDone());
            var batch = new Batch(List.of(new LogRecord(0, null, null, List.of())));
            runtime.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            assertEquals(1, waiting.get(2, TimeUnit.SECONDS).results().getFirst().batches().size());
            coordinator.close(); runtime.close();
        }
    }

    @Test
    void timeoutReturnsEmptySuccessAndReleasesWaiterCapacity() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 3), 0);
        var waiterBudget = new ResourceBudget(1);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(1, 1, 8);
             var store = FilePartitionStore.open(directory.resolve("partition"), config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, workers, clock, config);
            var coordinator = new FetchCoordinator(new FetchPlanner(Map.of(tp, runtime), config),
                    Map.of(tp, runtime)::get, clock, waiterBudget);
            var waiting = coordinator.fetch(new RequestContext(1, 8,
                    TimeUnit.SECONDS.toNanos(1)), new Fetch(1024, 1, 500,
                    List.of(new FetchEntry(tp, 0, 1024))));
            workers.drain().get(2, TimeUnit.SECONDS);
            assertEquals(1, waiterBudget.used());
            clock.advance(Duration.ofMillis(500));
            clock.runDue();
            assertTrue(waiting.get(2, TimeUnit.SECONDS).results().getFirst().batches().isEmpty());
            assertEquals(0, waiterBudget.used());
            coordinator.close(); runtime.close();
        }
    }

    @Test
    void alreadyDisconnectedContextDoesNotRetainWaiter() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 4), 0);
        var waiterBudget = new ResourceBudget(1);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(1, 1, 8);
             var store = FilePartitionStore.open(directory.resolve("partition"), config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, workers, clock, config);
            var coordinator = new FetchCoordinator(new FetchPlanner(Map.of(tp, runtime), config),
                    Map.of(tp, runtime)::get, clock, waiterBudget);
            var context = new RequestContext(1, 9, TimeUnit.SECONDS.toNanos(1));
            context.cancel();
            var waiting = coordinator.fetch(context, new Fetch(1024, 1, 500,
                    List.of(new FetchEntry(tp, 0, 1024))));
            assertTrue(waiting.isCompletedExceptionally());
            assertEquals(0, waiterBudget.used());
            assertEquals(0, clock.pending());
            coordinator.close(); runtime.close();
        }
    }
}

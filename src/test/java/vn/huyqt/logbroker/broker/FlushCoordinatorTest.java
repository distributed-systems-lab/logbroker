package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.protocol.Protocol.AckMode;
import vn.huyqt.logbroker.protocol.Protocol.Batch;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.ManualScheduler;

class FlushCoordinatorTest {
    @TempDir Path directory;

    @Test
    void repeatedAppendsDoNotMoveOldestFlushDeadline() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 2), 0);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(1, 1, 8);
             var store = FilePartitionStore.open(directory.resolve("partition"), config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, workers, clock, config);
            var batch = new Batch(List.of(new LogRecord(0, null, null, List.of())));
            runtime.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            clock.advance(Duration.ofMillis(9));
            runtime.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            clock.advance(Duration.ofMillis(1));
            clock.runDue();
            workers.drain().get(2, TimeUnit.SECONDS);
            assertEquals(2, store.durableEndOffset());
            runtime.close();
        }
    }

    @Test
    void byteThresholdFlushesBeforeTimeThreshold() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 3), 0);
        try (var clock = new ManualScheduler();
             var workers = new PartitionExecutor(1, 1, 8);
             var store = FilePartitionStore.open(directory.resolve("partition"), config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, workers, clock, config);
            var batch = new Batch(List.of(new LogRecord(0, null, new byte[600_000], List.of())));
            runtime.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            assertEquals(0, store.durableEndOffset());
            runtime.produce(batch, AckMode.APPENDED, TimeUnit.SECONDS.toNanos(1)).get();
            workers.drain().get(2, TimeUnit.SECONDS);
            assertEquals(2, store.durableEndOffset());
            assertEquals(0, clock.nanoTime());
            runtime.close();
        }
    }
}

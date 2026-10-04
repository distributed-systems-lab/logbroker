package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.protocol.Protocol.AckMode;
import vn.huyqt.logbroker.protocol.Protocol.Batch;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.storage.AppendResult;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.RecordBatch;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class ProduceDurabilityTest {
    @TempDir Path directory;

    @Test
    void flushedWaitsButAppendedDoesNotAndOneFlushCoversBoth() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 2), 0);
        try (var clock = new ManualScheduler();
                var workers = new PartitionExecutor(2, 2, 8);
                var store =
                        FilePartitionStore.open(
                                directory.resolve("partition"), config.logConfig())) {
            var runtime = new PartitionRuntime(tp, store, workers, clock, config);
            var batch = new Batch(List.of(new LogRecord(0, null, new byte[] {1}, List.of())));
            var appended =
                    runtime.produce(
                            batch,
                            AckMode.APPENDED,
                            clock.nanoTime() + TimeUnit.SECONDS.toNanos(1));
            var flushed =
                    runtime.produce(
                            batch, AckMode.FLUSHED, clock.nanoTime() + TimeUnit.SECONDS.toNanos(1));
            assertEquals(0, appended.get(2, TimeUnit.SECONDS).firstOffset());
            workers.drain().get(2, TimeUnit.SECONDS);
            assertFalse(flushed.isDone());
            assertEquals(0, store.durableEndOffset());
            clock.advance(Duration.ofMillis(10));
            clock.runDue();
            workers.drain().get(2, TimeUnit.SECONDS);
            assertEquals(2, flushed.get(2, TimeUnit.SECONDS).nextOffset());
            assertEquals(2, store.durableEndOffset());
            runtime.close();
        }
    }

    @Test
    void failedForceFailsFlushedWaiterWithoutClaimingDurability() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var tp = new TopicPartition(new UUID(1, 4), 0);
        try (var clock = new ManualScheduler();
                var workers = new PartitionExecutor(1, 1, 8);
                var backing =
                        FilePartitionStore.open(
                                directory.resolve("partition"), config.logConfig())) {
            PartitionStore failing =
                    new PartitionStore() {
                        public AppendResult append(List<LogRecord> records) throws IOException {
                            return backing.append(records);
                        }

                        public List<RecordBatch> read(long offset, int maxBytes)
                                throws IOException {
                            return backing.read(offset, maxBytes);
                        }

                        public long flush() throws IOException {
                            throw new IOException("force failed");
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
            var runtime = new PartitionRuntime(tp, failing, workers, clock, config);
            var batch = new Batch(List.of(new LogRecord(0, null, null, List.of())));
            var pending = runtime.produce(batch, AckMode.FLUSHED, TimeUnit.SECONDS.toNanos(1));
            workers.drain().get(2, TimeUnit.SECONDS);
            clock.advance(Duration.ofMillis(10));
            clock.runDue();
            workers.drain().get(2, TimeUnit.SECONDS);
            assertEquals(
                    vn.huyqt.logbroker.protocol.ErrorCode.STORAGE_ERROR,
                    pending.get(2, TimeUnit.SECONDS).error().code());
            assertEquals(0, backing.durableEndOffset());
            runtime.close();
        }
    }
}

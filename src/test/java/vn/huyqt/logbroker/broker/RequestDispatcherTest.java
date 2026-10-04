package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.metadata.LegacyMetadataFixture;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.ManualScheduler;

class RequestDispatcherTest {
    @TempDir Path directory;

    @Test
    void keepsSuccessfulPartitionWhenAnotherIsUnknown() throws Exception {
        var config = BrokerConfig.defaults(directory);
        try (var clock = new ManualScheduler();
                var workers = new PartitionExecutor(2, 2, 8);
                var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            UUID id = metadata.create("orders", 1).get().topicId();
            var known = new TopicPartition(id, 0);
            var missing = new TopicPartition(new UUID(5, 6), 0);
            var runtime =
                    new PartitionRuntime(known, registry.require(known), workers, clock, config);
            var runtimes = Map.of(known, runtime);
            var fetch =
                    new FetchCoordinator(
                            new FetchPlanner(runtimes, config),
                            runtimes::get,
                            clock,
                            new ResourceBudget(8));
            var dispatcher = new RequestDispatcher(metadata, runtimes::get, fetch, clock);
            var batch = new Batch(List.of(new LogRecord(0, null, null, List.of())));
            var context = new RequestContext(1, 7, TimeUnit.SECONDS.toNanos(1));
            var reply =
                    (ProduceReply)
                            dispatcher
                                    .handle(
                                            context,
                                            new Produce(
                                                    AckMode.APPENDED,
                                                    1000,
                                                    List.of(
                                                            new ProduceEntry(known, batch),
                                                            new ProduceEntry(missing, batch))))
                                    .get(2, TimeUnit.SECONDS);
            assertEquals(ErrorCode.NONE, reply.results().get(0).error().code());
            assertEquals(ErrorCode.UNKNOWN_PARTITION, reply.results().get(1).error().code());
            assertEquals(1, registry.require(known).logEndOffset());
            dispatcher.beginShutdown();
            fetch.close();
            runtime.close();
        }
    }
}

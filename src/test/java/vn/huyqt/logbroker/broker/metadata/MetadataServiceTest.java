package vn.huyqt.logbroker.broker.metadata;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.FilePartitionStore;
import vn.huyqt.logbroker.broker.PartitionRegistry;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class MetadataServiceTest {
    @TempDir Path directory;

    @Test
    void createIsDurableAndIdempotentAcrossRestart() throws Exception {
        var config = BrokerConfig.defaults(directory);
        UUID id;
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            id = metadata.create("orders", 2).get(5, TimeUnit.SECONDS).topicId();
            assertEquals(id, metadata.create("orders", 2).get(5, TimeUnit.SECONDS).topicId());
            assertEquals(
                    ErrorCode.TOPIC_ALREADY_EXISTS,
                    metadata.create("orders", 3).get(5, TimeUnit.SECONDS).error().code());
            assertNotNull(registry.require(new TopicPartition(id, 0)));
            assertNotNull(registry.require(new TopicPartition(id, 1)));
        }
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            var info = metadata.metadata(List.of("orders")).topics().getFirst();
            assertEquals(id, info.id());
            assertEquals(2, info.partitions().size());
        }
    }
}

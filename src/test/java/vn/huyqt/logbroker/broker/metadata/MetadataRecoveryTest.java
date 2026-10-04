package vn.huyqt.logbroker.broker.metadata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.FilePartitionStore;
import vn.huyqt.logbroker.broker.PartitionRegistry;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.PartitionLog;

class MetadataRecoveryTest {
    @TempDir Path directory;

    @Test
    void replaysCommittedTopicEvenWhenDataDirectoriesWereNeverCreated() throws Exception {
        var config = BrokerConfig.defaults(directory);
        var event = new TopicCatalog.TopicCreated(new UUID(3, 4), "orders", 2);
        try (var log = PartitionLog.open(directory.resolve("metadata"), config.logConfig())) {
            log.append(
                    List.of(new LogRecord(0, null, MetadataEventCodec.encode(event), List.of())));
            log.flush();
        }
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            assertEquals(event.id(), metadata.metadata(List.of("orders")).topics().getFirst().id());
            assertNotNull(
                    registry.require(
                            new vn.huyqt.logbroker.protocol.Protocol.TopicPartition(
                                    event.id(), 1)));
        }
    }

    @Test
    void corruptedMetadataStopsStartup() throws Exception {
        var config = BrokerConfig.defaults(directory);
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            metadata.create("orders", 1).get();
        }
        Path data = directory.resolve("metadata").resolve("00000000000000000000.log");
        byte[] bytes = Files.readAllBytes(data);
        bytes[bytes.length - 1] ^= 1;
        Files.write(data, bytes);
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open)) {
            assertThrows(
                    IOException.class,
                    () -> LegacyMetadataFixture.open(directory, config, registry));
        }
    }

    @Test
    void damagedDataPartitionDoesNotHideHealthySibling() throws Exception {
        var config = BrokerConfig.defaults(directory);
        UUID id;
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            id = metadata.create("orders", 2).get().topicId();
            registry.require(new vn.huyqt.logbroker.protocol.Protocol.TopicPartition(id, 0))
                    .append(List.of(new LogRecord(0, null, new byte[] {1}, List.of())));
        }
        Path data =
                directory
                        .resolve("topics")
                        .resolve(id.toString())
                        .resolve("0")
                        .resolve("00000000000000000000.log");
        byte[] bytes = Files.readAllBytes(data);
        bytes[bytes.length - 1] ^= 1;
        Files.write(data, bytes);
        try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
                var metadata = LegacyMetadataFixture.open(directory, config, registry)) {
            var topics = metadata.metadata(List.of("orders")).topics();
            assertEquals(
                    vn.huyqt.logbroker.protocol.ErrorCode.PARTITION_UNAVAILABLE,
                    topics.getFirst().partitions().get(0).error().code());
            assertEquals(
                    vn.huyqt.logbroker.protocol.ErrorCode.NONE,
                    topics.getFirst().partitions().get(1).error().code());
        }
    }
}

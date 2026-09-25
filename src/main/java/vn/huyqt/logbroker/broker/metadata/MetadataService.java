package vn.huyqt.logbroker.broker.metadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.PartitionRegistry;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.CreateTopicReply;
import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.protocol.Protocol.MetadataReply;
import vn.huyqt.logbroker.protocol.Protocol.PartitionInfo;
import vn.huyqt.logbroker.protocol.Protocol.TopicInfo;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.PartitionLog;

/** Local event source for topic metadata; KRaft will replace this source later. */
public final class MetadataService implements AutoCloseable {
    private final BrokerConfig config;
    private final PartitionRegistry registry;
    private final PartitionLog log;
    private final TopicCatalog catalog = new TopicCatalog();
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0,
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(256), action -> {
                var thread = new Thread(action, "broker-metadata");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
    private final Consumer<Throwable> fatalHandler;
    private volatile boolean failed;
    private volatile boolean closed;

    private MetadataService(BrokerConfig config, PartitionRegistry registry,
                            PartitionLog log, Consumer<Throwable> fatalHandler) {
        this.config = config;
        this.registry = registry;
        this.log = log;
        this.fatalHandler = fatalHandler;
    }

    public static MetadataService open(Path root, BrokerConfig config,
                                       PartitionRegistry registry) throws IOException {
        return open(root, config, registry, error -> {});
    }

    public static MetadataService open(Path root, BrokerConfig config,
                                       PartitionRegistry registry,
                                       Consumer<Throwable> fatalHandler) throws IOException {
        Objects.requireNonNull(root); Objects.requireNonNull(config);
        Objects.requireNonNull(registry); Objects.requireNonNull(fatalHandler);
        PartitionLog log = PartitionLog.open(root.resolve("metadata"), config.logConfig());
        MetadataService service = new MetadataService(config, registry, log, fatalHandler);
        try {
            service.replay();
            for (TopicInfo topic : service.catalog.snapshot()) registry.initialize(topic);
            return service;
        } catch (IOException | RuntimeException error) {
            service.worker.shutdownNow();
            try { log.close(); } catch (IOException closeError) { error.addSuppressed(closeError); }
            throw error;
        }
    }

    private void replay() throws IOException {
        long offset = log.logStartOffset();
        while (offset < log.logEndOffset()) {
            List<vn.huyqt.logbroker.storage.RecordBatch> batches =
                    log.read(offset, config.logConfig().maxBatchBytes());
            if (batches.isEmpty()) throw new IOException("Metadata replay made no progress");
            for (var batch : batches) {
                for (LogRecord record : batch.records()) {
                    if (record.timestamp() != 0 || record.key() != null
                            || !record.headers().isEmpty())
                        throw new IOException("Invalid metadata record");
                    catalog.apply(MetadataEventCodec.decode(record.value()));
                }
                offset = batch.nextOffset();
            }
        }
        if (catalog.topicCount() > config.maxTopics()
                || catalog.partitionCount() > config.maxPartitions())
            throw new IOException("Metadata exceeds broker limits");
    }

    public CompletableFuture<CreateTopicReply> create(String name, int partitions) {
        var result = new CompletableFuture<CreateTopicReply>();
        if (closed || failed) {
            result.complete(new CreateTopicReply(
                    new Error(ErrorCode.BROKER_SHUTTING_DOWN, "Metadata unavailable"),
                    new UUID(0, 0)));
            return result;
        }
        try {
            worker.execute(() -> {
                try { result.complete(createOnWorker(name, partitions)); }
                catch (Throwable error) { result.completeExceptionally(error); }
            });
        } catch (java.util.concurrent.RejectedExecutionException error) {
            result.complete(new CreateTopicReply(new Error(ErrorCode.OVERLOADED,
                    "Metadata queue full"), new UUID(0, 0)));
        }
        return result;
    }

    private CreateTopicReply createOnWorker(String name, int partitions) {
        if (closed || failed) return new CreateTopicReply(
                new Error(ErrorCode.BROKER_SHUTTING_DOWN, "Metadata unavailable"), new UUID(0, 0));
        if (name == null || !name.matches("[A-Za-z0-9._-]{1,249}")
                || name.equals(".") || name.equals("..")
                || partitions < 1 || partitions > config.maxPartitions())
            return new CreateTopicReply(new Error(ErrorCode.INVALID_REQUEST,
                    "Invalid topic name/count"), new UUID(0, 0));
        var existing = catalog.find(name);
        if (existing != null) {
            if (existing.partitions() != partitions) return new CreateTopicReply(
                    new Error(ErrorCode.TOPIC_ALREADY_EXISTS, "Partition count differs"), existing.id());
            return new CreateTopicReply(topicError(existing.id(), partitions), existing.id());
        }
        if (catalog.topicCount() >= config.maxTopics()
                || partitions > config.maxPartitions() - catalog.partitionCount())
            return new CreateTopicReply(new Error(ErrorCode.OVERLOADED,
                    "Topic capacity reached"), new UUID(0, 0));
        UUID id = UUID.randomUUID();
        if (id.equals(new UUID(0, 0))) id = UUID.randomUUID();
        var event = new TopicCatalog.TopicCreated(id, name, partitions);
        try {
            log.append(List.of(new LogRecord(0, null,
                    MetadataEventCodec.encode(event), List.of())));
            log.flush();
            catalog.apply(event);
            registry.initialize(topicInfo(event));
            return new CreateTopicReply(topicError(id, partitions), id);
        } catch (IOException error) {
            failed = true;
            fatalHandler.accept(error);
            return new CreateTopicReply(new Error(ErrorCode.STORAGE_ERROR,
                    "Metadata write failed"), id);
        }
    }

    private Error topicError(UUID id, int partitions) {
        for (int i = 0; i < partitions; i++) {
            if (registry.state(new TopicPartition(id, i)) != ErrorCode.NONE)
                return new Error(ErrorCode.PARTITION_UNAVAILABLE,
                        "Topic partition unavailable");
        }
        return Error.none();
    }

    private static TopicInfo topicInfo(TopicCatalog.TopicCreated event) {
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < event.partitions(); i++)
            partitions.add(new PartitionInfo(i, Error.none()));
        return new TopicInfo(event.name(), event.id(), partitions);
    }

    public MetadataReply metadata(List<String> names) {
        if (names == null || names.size() > config.maxTopics()
                || new HashSet<>(names).size() != names.size())
            return new MetadataReply(new Error(ErrorCode.INVALID_REQUEST,
                    "Invalid metadata query"), config.host(), config.port(), List.of());
        List<TopicInfo> topics = new ArrayList<>();
        for (TopicInfo topic : catalog.snapshot()) {
            if (!names.isEmpty() && !names.contains(topic.name())) continue;
            List<PartitionInfo> partitions = new ArrayList<>();
            for (var partition : topic.partitions()) {
                ErrorCode state = registry.state(new TopicPartition(topic.id(), partition.partition()));
                partitions.add(new PartitionInfo(partition.partition(),
                        new Error(state, state == ErrorCode.NONE ? "" : "Partition unavailable")));
            }
            topics.add(new TopicInfo(topic.name(), topic.id(), partitions));
        }
        if (!names.isEmpty() && topics.size() != names.size())
            return new MetadataReply(new Error(ErrorCode.UNKNOWN_TOPIC,
                    "Unknown topic"), config.host(), config.port(), List.of());
        return new MetadataReply(Error.none(), config.host(), config.port(), topics);
    }

    @Override public void close() throws IOException {
        closed = true;
        worker.shutdown();
        try {
            if (!worker.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS))
                throw new IOException("Metadata worker did not stop before deadline");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted closing metadata", error);
        } finally {
            if (worker.isTerminated()) log.close();
        }
    }
}

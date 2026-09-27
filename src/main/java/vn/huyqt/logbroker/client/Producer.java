package vn.huyqt.logbroker.client;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.RecordPayloadCodec;

/**
 * Per-partition ordered batches with explicit ACK mode and bounded caller
 * buffer.
 */
public final class Producer implements AutoCloseable {
    private final BrokerClient client;
    private final ClientConfig config;
    private final DeadlineScheduler clock;
    private final ResourceBudget queued;
    private final Map<String, CompletableFuture<Protocol.TopicInfo>> topics = new HashMap<>();
    private final Map<Protocol.TopicPartition, Lane> lanes = new HashMap<>();
    private final Map<String, Integer> nullCursors = new HashMap<>();
    private final Set<CompletableFuture<RecordMetadata>> outstanding = ConcurrentHashMap.newKeySet();
    private boolean closed;

    public Producer(BrokerClient client, ClientConfig config, DeadlineScheduler clock) {
        this.client = Objects.requireNonNull(client);
        this.config = Objects.requireNonNull(config);
        this.clock = Objects.requireNonNull(clock);
        queued = new ResourceBudget(config.queuedBytes());
    }

    public CompletableFuture<RecordMetadata> send(String topic, Integer partition,
            LogRecord record, Protocol.AckMode ack) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(record);
        Objects.requireNonNull(ack);
        var result = new CompletableFuture<RecordMetadata>();
        final int bytes;
        try {
            bytes = Math.addExact(22, RecordPayloadCodec.encodedSize(List.of(record)));
        } catch (RuntimeException invalid) {
            return CompletableFuture.failedFuture(invalid);
        }
        if (bytes > ProtocolLimits.defaults().maxWireBatchBytes())
            return CompletableFuture.failedFuture(ClientException.notSent("Record too large"));
        ResourceBudget.Lease lease = queued.reserve(bytes).orElse(null);
        if (lease == null)
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Producer buffer full"));
        long deadline = clock.nanoTime() + config.requestTimeout().toNanos();
        CompletableFuture<Protocol.TopicInfo> metadata;
        synchronized (this) {
            if (closed) {
                lease.close();
                return CompletableFuture.failedFuture(
                        ClientException.notSent("Producer closed"));
            }
            outstanding.add(result);
            result.whenComplete((ignored, error) -> {
                lease.close();
                outstanding.remove(result);
            });
            metadata = topics.computeIfAbsent(topic, this::lookup);
        }
        metadata.whenComplete((info, error) -> {
            if (error != null) {
                lease.close();
                result.completeExceptionally(error);
                return;
            }
            synchronized (Producer.this) {
                if (result.isDone() || closed || clock.nanoTime() >= deadline) {
                    lease.close();
                    result.completeExceptionally(ClientException.notSent("Record expired before send"));
                    return;
                }
                int count = info.partitions().size();
                int selected = partition != null ? partition
                        : record.key() != null ? Partitioner.forKey(record.key(), count)
                                : nullCursors.getOrDefault(topic, 0) % count;
                if (selected < 0 || selected >= count ||
                        info.partitions().get(selected).error().code() != vn.huyqt.logbroker.protocol.ErrorCode.NONE) {
                    lease.close();
                    result.completeExceptionally(ClientException.notSent("Unknown partition"));
                    return;
                }
                var tp = new Protocol.TopicPartition(info.id(), selected);
                var lane = lanes.computeIfAbsent(tp, ignored -> new Lane());
                if (lane.open != null && (lane.open.ack != ack
                        || lane.open.bytes + bytes > config.targetBatchBytes()
                        || lane.open.records.size() >= 10_000))
                    seal(tp, lane, topic);
                if (lane.open == null) {
                    lane.open = new BatchAccumulator(tp, ack);
                    var open = lane.open;
                    open.linger = clock.schedule(clock.nanoTime() + config.linger().toNanos(),
                            () -> {
                                synchronized (Producer.this) {
                                    if (lane.open == open)
                                        seal(tp, lane, topic);
                                }
                            });
                }
                lane.open.add(record, result, lease, deadline, bytes);
                lane.open.usesNullRouting |= partition == null && record.key() == null;
                if (lane.open.bytes >= config.targetBatchBytes()
                        || lane.open.records.size() >= 10_000)
                    seal(tp, lane, topic);
            }
        });
        return result;
    }

    private CompletableFuture<Protocol.TopicInfo> lookup(String topic) {
        return client.request(new Protocol.Metadata(List.of(topic))).thenApply(reply -> {
            if (!(reply instanceof Protocol.MetadataReply metadata))
                throw ClientException.notSent("Metadata request failed");
            return metadata.topics().stream().filter(info -> info.name().equals(topic))
                    .findFirst().orElseThrow(() -> ClientException.notSent("Unknown topic"));
        }).whenComplete((ignored, error) -> {
            if (error != null)
                synchronized (Producer.this) {
                    topics.remove(topic);
                }
        });
    }

    private void seal(Protocol.TopicPartition tp, Lane lane, String topic) {
        var batch = lane.open;
        if (batch == null)
            return;
        lane.open = null;
        batch.linger.cancel();
        lane.ready.addLast(batch);
        if (batch.usesNullRouting)
            nullCursors.merge(topic, 1, Integer::sum);
        drain(lane);
    }

    private void drain(Lane lane) {
        if (lane.busy)
            return;
        var batch = lane.ready.pollFirst();
        if (batch == null)
            return;
        var records = batch.liveRecords(clock.nanoTime());
        if (records.isEmpty()) {
            drain(lane);
            return;
        }
        lane.busy = true;
        int timeout = (int) Math.min(Integer.MAX_VALUE, config.requestTimeout().toMillis());
        client.request(new Protocol.Produce(batch.ack, timeout,
                List.of(new Protocol.ProduceEntry(batch.partition, new Protocol.Batch(records)))))
                .whenComplete((reply, error) -> {
                    Protocol.ProduceResult outcome = null;
                    if (error == null && reply instanceof Protocol.ProduceReply produced
                            && !produced.results().isEmpty())
                        outcome = produced.results().getFirst();
                    if (error == null && outcome == null)
                        error = ClientException.unknown(new IllegalStateException("Missing Produce result"));
                    batch.complete(outcome, error);
                    synchronized (Producer.this) {
                        lane.busy = false;
                        drain(lane);
                    }
                });
    }

    @Override
    public void close() {
        CompletableFuture<?>[] accepted;
        synchronized (this) {
            if (!closed) {
                closed = true;
                for (var entry : lanes.entrySet())
                    seal(entry.getKey(), entry.getValue(), "");
            }
            accepted = outstanding.toArray(CompletableFuture[]::new);
        }
        if (accepted.length == 0)
            return;
        try {
            CompletableFuture.allOf(accepted).get(config.requestTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException deadline) {
            for (var pending : outstanding)
                pending.completeExceptionally(
                        ClientException.unknown(deadline));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            for (var pending : outstanding)
                pending.completeExceptionally(
                        ClientException.unknown(interrupted));
        } catch (java.util.concurrent.ExecutionException completedWithErrors) {
            // All accepted requests settled; callers retain their individual failures.
        }
    }

    public record RecordMetadata(Protocol.TopicPartition partition, long offset) {
    }

    private static final class Lane {
        BatchAccumulator open;
        final ArrayDeque<BatchAccumulator> ready = new ArrayDeque<>();
        boolean busy;
    }
}

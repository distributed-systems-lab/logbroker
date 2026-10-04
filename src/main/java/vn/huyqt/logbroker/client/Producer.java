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
 * Per-partition ordered batches with explicit ACK mode and bounded caller buffer.
 *
 * <p>Records are grouped per topic partition. Each partition has at most one Produce request in
 * flight, so sealed batches of a partition reach the broker in the order they were sealed. The
 * producer keeps the lane occupied while its client retries explicitly rejected requests. An
 * uncertain append is never retried; see {@link ClientException.Outcome#UNKNOWN}. Topic metadata is
 * looked up by name once and cached; a failed lookup is evicted so a later send tries again.
 *
 * <p>The producer shares the {@link RequestClient} it is given and does not close it. All mutable
 * state is guarded by the producer's monitor, so {@link #send} may be called from any thread.
 */
public final class Producer implements AutoCloseable {
    private final RequestClient client;
    private final ClientConfig config;
    private final DeadlineScheduler clock;
    private final ResourceBudget queued;
    private final Map<String, CompletableFuture<Protocol.TopicInfo>> topics = new HashMap<>();
    private final Map<Protocol.TopicPartition, Lane> lanes = new HashMap<>();
    private final Map<String, Integer> nullCursors = new HashMap<>();
    private final Set<CompletableFuture<RecordMetadata>> outstanding =
            ConcurrentHashMap.newKeySet();
    private boolean closed;

    public Producer(RequestClient client, ClientConfig config, DeadlineScheduler clock) {
        this.client = Objects.requireNonNull(client);
        this.config = Objects.requireNonNull(config);
        this.clock = Objects.requireNonNull(clock);
        queued = new ResourceBudget(config.queuedBytes());
    }

    /**
     * Queues one record and returns a future for its offset.
     *
     * <p>Routing: an explicit {@code partition} wins; otherwise a non-null key is hashed with
     * {@link Partitioner}; otherwise records go round-robin per batch, advancing a per-topic cursor
     * each time a batch that used null routing is sealed. A batch is sealed when it reaches {@link
     * ClientConfig#targetBatchBytes()} or 10,000 records, when its linger expires, or when the next
     * record uses a different {@code ack} mode.
     *
     * <p>The record's deadline is {@link ClientConfig#requestTimeout()} from this call and covers
     * metadata lookup, batching and sending. The future completes exceptionally with {@link
     * ClientException.Outcome#NOT_SENT} if the record is too large, the producer buffer is full,
     * the producer is closed, the partition is unknown or unavailable, or the deadline passes
     * before the batch is sent. Failures of the metadata lookup and of the Produce request from
     * {@link BrokerClient#request} are passed through; a missing Produce result or an entry error
     * is {@link ClientException.Outcome#UNKNOWN}. A {@code FLUSHED} success means the broker's
     * local durability marker covers the record; neither mode is replication.
     *
     * @param partition explicit partition ID, or {@code null} to route by key
     */
    public CompletableFuture<RecordMetadata> send(
            String topic, Integer partition, LogRecord record, Protocol.AckMode ack) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(record);
        Objects.requireNonNull(ack);
        var result = new CompletableFuture<RecordMetadata>();
        final int bytes;
        try {
            // 14-byte wire batch header plus 8-byte baseOffset: the wire cap includes baseOffset
            // (docs/protocol-v1.md), so a single record must fit a batch of its own.
            bytes = Math.addExact(22, RecordPayloadCodec.encodedSize(List.of(record)));
        } catch (RuntimeException invalid) {
            return CompletableFuture.failedFuture(invalid);
        }
        if (bytes > ProtocolLimits.defaults().maxWireBatchBytes())
            return CompletableFuture.failedFuture(ClientException.notSent("Record too large"));
        ResourceBudget.Lease lease = queued.reserve(bytes).orElse(null);
        if (lease == null)
            return CompletableFuture.failedFuture(ClientException.notSent("Producer buffer full"));
        long deadline = clock.nanoTime() + config.requestTimeout().toNanos();
        CompletableFuture<Protocol.TopicInfo> metadata;
        synchronized (this) {
            if (closed) {
                lease.close();
                return CompletableFuture.failedFuture(ClientException.notSent("Producer closed"));
            }
            outstanding.add(result);
            result.whenComplete(
                    (ignored, error) -> {
                        lease.close();
                        outstanding.remove(result);
                    });
            metadata = topics.computeIfAbsent(topic, name -> lookup(name, deadline));
        }
        metadata.whenComplete(
                (info, error) -> {
                    if (error != null) {
                        lease.close();
                        result.completeExceptionally(error);
                        return;
                    }
                    synchronized (Producer.this) {
                        if (result.isDone() || closed || clock.nanoTime() >= deadline) {
                            lease.close();
                            result.completeExceptionally(
                                    ClientException.notSent("Record expired before send"));
                            return;
                        }
                        int count = info.partitions().size();
                        int selected =
                                partition != null
                                        ? partition
                                        : record.key() != null
                                                ? Partitioner.forKey(record.key(), count)
                                                : nullCursors.getOrDefault(topic, 0) % count;
                        if (selected < 0
                                || selected >= count
                                || info.partitions().get(selected).error().code()
                                        != vn.huyqt.logbroker.protocol.ErrorCode.NONE) {
                            lease.close();
                            result.completeExceptionally(
                                    ClientException.notSent("Unknown partition"));
                            return;
                        }
                        var tp = new Protocol.TopicPartition(info.id(), selected);
                        var lane = lanes.computeIfAbsent(tp, ignored -> new Lane());
                        // A Produce request carries a single ack mode, so a mode change seals the
                        // batch.
                        if (lane.open != null
                                && (lane.open.ack != ack
                                        || lane.open.bytes + bytes > config.targetBatchBytes()
                                        || lane.open.records.size() >= 10_000))
                            seal(tp, lane, topic);
                        if (lane.open == null) {
                            lane.open = new BatchAccumulator(tp, ack);
                            var open = lane.open;
                            open.linger =
                                    clock.schedule(
                                            clock.nanoTime() + config.linger().toNanos(),
                                            () -> {
                                                synchronized (Producer.this) {
                                                    // The batch may already be sealed by size;
                                                    // never seal a
                                                    // newer batch from this timer.
                                                    if (lane.open == open) seal(tp, lane, topic);
                                                }
                                            });
                        }
                        lane.open.add(record, result, lease, deadline, bytes);
                        lane.open.usesNullRouting |= partition == null && record.key() == null;
                        if (lane.open.bytes >= config.targetBatchBytes()
                                || lane.open.records.size() >= 10_000) seal(tp, lane, topic);
                    }
                });
        return result;
    }

    private CompletableFuture<Protocol.TopicInfo> lookup(String topic, long deadline) {
        return client.request(new Protocol.Metadata(List.of(topic)), deadline)
                .thenApply(
                        reply -> {
                            if (reply
                                    instanceof
                                    vn.huyqt.logbroker.protocol.ClusterProtocol.MetadataReply
                                                    metadata) {
                                var found =
                                        metadata.topics().stream()
                                                .filter(info -> info.name().equals(topic))
                                                .findFirst()
                                                .orElseThrow(
                                                        () ->
                                                                ClientException.notSent(
                                                                        "Unknown topic"));
                                return new Protocol.TopicInfo(
                                        found.name(),
                                        found.id(),
                                        found.partitions().stream()
                                                .sorted(
                                                        java.util.Comparator.comparingInt(
                                                                vn.huyqt.logbroker.protocol
                                                                                .ClusterProtocol
                                                                                .PartitionInfo
                                                                        ::partition))
                                                .map(
                                                        partition ->
                                                                new Protocol.PartitionInfo(
                                                                        partition.partition(),
                                                                        partition.error()))
                                                .toList());
                            }
                            if (!(reply instanceof Protocol.MetadataReply metadata))
                                throw ClientException.notSent("Metadata request failed");
                            return metadata.topics().stream()
                                    .filter(info -> info.name().equals(topic))
                                    .findFirst()
                                    .orElseThrow(() -> ClientException.notSent("Unknown topic"));
                        })
                .whenComplete(
                        (ignored, error) -> {
                            // Evict a failed lookup so the next send looks the topic up again
                            // instead of
                            // reusing the failed future.
                            if (error != null)
                                synchronized (Producer.this) {
                                    topics.remove(topic);
                                }
                        });
    }

    /**
     * Closes the lane's open batch to new records and queues it for sending, then tries to send.
     * Does nothing if the lane has no open batch.
     *
     * <p>Must be called while holding the producer's monitor. {@code topic} only selects the
     * null-routing cursor to advance.
     */
    private void seal(Protocol.TopicPartition tp, Lane lane, String topic) {
        var batch = lane.open;
        if (batch == null) return;
        // The next record for this lane must start a new batch.
        lane.open = null;
        // The batch is no longer open, so its linger timer must not fire for it.
        batch.linger.cancel();
        lane.ready.addLast(batch);
        // Round-robin advances per sealed batch, not per record: all null-routed records of this
        // batch went to the same partition, so the next null-routed batch moves to the next one.
        if (batch.usesNullRouting) nullCursors.merge(topic, 1, Integer::sum);
        drain(lane);
    }

    /**
     * Sends the next queued batch of the lane if no Produce is in flight for it. Called after a
     * batch is sealed and again when the previous Produce completes, so queued batches are sent one
     * at a time.
     *
     * <p>Must be called while holding the producer's monitor.
     */
    private void drain(Lane lane) {
        // At most one Produce in flight per partition: the next batch is sent only after the
        // previous one completes, so a partition's batches are sent in seal order.
        if (lane.busy) return;
        var batch = lane.ready.pollFirst();
        if (batch == null) return;
        // Drops cancelled records and fails expired ones (NOT_SENT) before anything hits the wire.
        var records = batch.liveRecords(clock.nanoTime());
        if (records.isEmpty()) {
            // Nothing left to send in this batch; move on to the next queued one.
            drain(lane);
            return;
        }
        lane.busy = true;
        long deadline =
                batch.sent.stream().mapToLong(BatchAccumulator.Item::deadline).min().orElseThrow();
        int timeout = (int) Math.max(1, Math.min(30000, (deadline - clock.nanoTime()) / 1_000_000));
        client.request(
                        new Protocol.Produce(
                                batch.ack,
                                timeout,
                                List.of(
                                        new Protocol.ProduceEntry(
                                                batch.partition, new Protocol.Batch(records)))),
                        deadline)
                .whenComplete(
                        (reply, error) -> {
                            Protocol.ProduceResult outcome = null;
                            if (error == null
                                    && reply instanceof Protocol.ProduceReply produced
                                    && !produced.results().isEmpty())
                                outcome = produced.results().getFirst();
                            if (error == null
                                    && reply
                                            instanceof
                                            vn.huyqt.logbroker.protocol.ClusterProtocol.ProduceReply
                                                            produced
                                    && produced.results().size() == 1
                                    && produced.results()
                                            .getFirst()
                                            .partition()
                                            .equals(batch.partition)) {
                                var result = produced.results().getFirst();
                                if (result.outcome()
                                        == vn.huyqt.logbroker.protocol.ClusterProtocol.Outcome
                                                .SUCCESS)
                                    outcome =
                                            new Protocol.ProduceResult(
                                                    result.partition(),
                                                    result.error(),
                                                    result.firstOffset(),
                                                    result.nextOffset());
                                else
                                    error =
                                            new ClientException(
                                                    result.retrySafe()
                                                            ? ClientException.Outcome.NOT_SENT
                                                            : ClientException.Outcome.UNKNOWN,
                                                    result.error().code(),
                                                    result.error().message(),
                                                    null);
                            }
                            // A reply without a result for our single entry leaves the outcome
                            // unknown.
                            if (error == null && outcome == null)
                                error =
                                        ClientException.unknown(
                                                new IllegalStateException(
                                                        "Missing Produce result"));
                            // Completes the callers' futures outside the monitor so their callbacks
                            // do
                            // not run while the producer is locked.
                            batch.complete(outcome, error);
                            synchronized (Producer.this) {
                                // Free the lane and send the next queued batch, if any.
                                lane.busy = false;
                                drain(lane);
                            }
                        });
    }

    /**
     * Stops accepting sends, seals every open batch for sending, and waits up to {@link
     * ClientConfig#requestTimeout()} for accepted records. Records still pending after the wait, or
     * when the calling thread is interrupted, complete with {@link
     * ClientException.Outcome#UNKNOWN}; the interrupt flag is restored. Records still waiting for
     * topic metadata are not sent: a successful lookup after close fails them with {@link
     * ClientException.Outcome#NOT_SENT}. Close never upgrades an {@code APPENDED} record to {@code
     * FLUSHED}, and it does not close the {@link BrokerClient}. Repeated calls only wait again.
     */
    @Override
    public void close() {
        CompletableFuture<?>[] accepted;
        synchronized (this) {
            if (!closed) {
                closed = true;
                // The topic argument only advances the null-routing cursor, unused once closed.
                for (var entry : lanes.entrySet()) seal(entry.getKey(), entry.getValue(), "");
            }
            accepted = outstanding.toArray(CompletableFuture[]::new);
        }
        if (accepted.length == 0) return;
        try {
            CompletableFuture.allOf(accepted)
                    .get(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException deadline) {
            for (var pending : outstanding)
                pending.completeExceptionally(ClientException.unknown(deadline));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            for (var pending : outstanding)
                pending.completeExceptionally(ClientException.unknown(interrupted));
        } catch (java.util.concurrent.ExecutionException completedWithErrors) {
            // All accepted requests settled; callers retain their individual failures.
        }
    }

    /** Partition and log offset assigned to one successfully produced record. */
    public record RecordMetadata(Protocol.TopicPartition partition, long offset) {}

    /** Per-partition state: the open batch, sealed batches waiting to send, and a busy flag. */
    private static final class Lane {
        BatchAccumulator open;
        final ArrayDeque<BatchAccumulator> ready = new ArrayDeque<>();
        boolean busy;
    }
}

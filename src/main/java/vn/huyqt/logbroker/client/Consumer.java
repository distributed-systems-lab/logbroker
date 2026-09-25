package vn.huyqt.logbroker.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;

/** Explicit-offset Fetch; no commit, reset, or group state. */
public final class Consumer {
    private final BrokerClient client;
    private int rotation;

    public Consumer(BrokerClient client) { this.client = Objects.requireNonNull(client); }

    public synchronized CompletableFuture<List<PartitionRecords>> fetch(
            List<Protocol.FetchEntry> entries, int maxBytes, int minBytes, int maxWaitMs) {
        if (entries.isEmpty()) return CompletableFuture.completedFuture(List.of());
        int start = rotation++ % entries.size();
        var ordered = new ArrayList<Protocol.FetchEntry>(entries.size());
        ordered.addAll(entries.subList(start, entries.size()));
        ordered.addAll(entries.subList(0, start));
        Map<Protocol.TopicPartition, Long> offsets = new HashMap<>();
        for (var entry : entries) offsets.put(entry.partition(), entry.offset());
        return client.request(new Protocol.Fetch(maxBytes, minBytes, maxWaitMs, ordered))
                .thenApply(response -> {
                    if (!(response instanceof Protocol.FetchReply reply))
                        throw new IllegalStateException("Unexpected Fetch response: " + response);
                    var results = new ArrayList<PartitionRecords>();
                    for (var partition : reply.results()) {
                        long requested = offsets.getOrDefault(partition.partition(), Long.MIN_VALUE);
                        var records = new ArrayList<FetchedRecord>();
                        for (var batch : partition.batches()) {
                            long offset = batch.baseOffset();
                            for (var record : batch.batch().records()) {
                                if (offset >= requested) records.add(new FetchedRecord(offset, record));
                                offset++;
                            }
                        }
                        results.add(new PartitionRecords(partition.partition(), partition.error(),
                                partition.logStartOffset(), partition.logEndOffset(), records));
                    }
                    return List.copyOf(results);
                });
    }

    public record FetchedRecord(long offset, LogRecord record) {}
    public record PartitionRecords(Protocol.TopicPartition partition, Protocol.Error error,
                                   long start, long end, List<FetchedRecord> records) {
        public PartitionRecords { records = List.copyOf(records); }
    }
}

package vn.huyqt.logbroker.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;

/**
 * Explicit-offset Fetch; no commit, reset, or group state.
 *
 * <p>The caller owns consumer progress and chooses the next offset from the returned records and
 * log bounds. The consumer never skips a failed offset or resets to either end of the log. It
 * shares the {@link BrokerClient} it is given and does not close it. {@link #fetch} is synchronized
 * only to advance the rotation cursor; the request itself runs asynchronously.
 */
public final class Consumer {
    private final RequestClient client;
    private int rotation;

    public Consumer(RequestClient client) {
        this.client = Objects.requireNonNull(client);
    }

    /**
     * Sends one Fetch for {@code entries} and returns per-partition records at or after each
     * requested offset. Limits and long-poll semantics are those of Fetch in {@code
     * docs/protocol-v1.md}; an empty list completes immediately without a request.
     *
     * <p>Each call rotates the order of {@code entries} by one position, because the broker grants
     * byte budget to partitions in request order and a fixed order could starve later entries. A
     * partition that received no budget is returned as a successful empty result, which does not
     * mean the end of the log was reached.
     *
     * @return future that completes exceptionally with the {@link BrokerClient#request} failure, or
     *     with {@link IllegalStateException} if the broker replies with a response other than
     *     {@link Protocol.FetchReply}, such as a top-level {@link Protocol.Failure}
     */
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
                .thenApply(
                        response -> {
                            if (response
                                    instanceof
                                    vn.huyqt.logbroker.protocol.ClusterProtocol.FetchReply
                                                    cluster) {
                                var results = new ArrayList<PartitionRecords>();
                                for (var partition : cluster.results()) {
                                    long requested =
                                            offsets.getOrDefault(
                                                    partition.partition(), Long.MIN_VALUE);
                                    var records = new ArrayList<FetchedRecord>();
                                    for (var batch : partition.batches()) {
                                        long offset = batch.baseOffset();
                                        for (var record : batch.batch().records()) {
                                            if (offset >= requested)
                                                records.add(new FetchedRecord(offset, record));
                                            offset++;
                                        }
                                    }
                                    results.add(
                                            new PartitionRecords(
                                                    partition.partition(),
                                                    partition.error(),
                                                    partition.logStartOffset(),
                                                    partition.highWatermark(),
                                                    records));
                                }
                                return List.copyOf(results);
                            }
                            if (!(response instanceof Protocol.FetchReply reply))
                                throw new IllegalStateException(
                                        "Unexpected Fetch response: " + response);
                            var results = new ArrayList<PartitionRecords>();
                            for (var partition : reply.results()) {
                                long requested =
                                        offsets.getOrDefault(partition.partition(), Long.MIN_VALUE);
                                var records = new ArrayList<FetchedRecord>();
                                // The first batch may begin before the requested offset; offsets
                                // are
                                // assigned per record from baseOffset and earlier records are
                                // dropped.
                                for (var batch : partition.batches()) {
                                    long offset = batch.baseOffset();
                                    for (var record : batch.batch().records()) {
                                        if (offset >= requested)
                                            records.add(new FetchedRecord(offset, record));
                                        offset++;
                                    }
                                }
                                results.add(
                                        new PartitionRecords(
                                                partition.partition(),
                                                partition.error(),
                                                partition.logStartOffset(),
                                                partition.logEndOffset(),
                                                records));
                            }
                            return List.copyOf(results);
                        });
    }

    /** A record with the log offset derived from its batch base offset. */
    public record FetchedRecord(long offset, LogRecord record) {}

    /**
     * Fetch result for one partition.
     *
     * @param error entry-level error; other partitions in the same response are unaffected
     * @param start log start offset reported by the broker, or -1 on entry error
     * @param end log end offset reported by the broker, or -1 on entry error; records up to it are
     *     readable even if not yet flushed
     * @param records records at or after the requested offset
     */
    public record PartitionRecords(
            Protocol.TopicPartition partition,
            Protocol.Error error,
            long start,
            long end,
            List<FetchedRecord> records) {
        public PartitionRecords {
            records = List.copyOf(records);
        }
    }
}

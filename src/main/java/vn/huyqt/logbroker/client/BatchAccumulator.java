package vn.huyqt.logbroker.client;

import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * One sealed wire batch and its individual record completions.
 *
 * <p>Owned by one {@link Producer} partition lane. Records are added and filtered while holding the
 * producer's monitor; {@code complete} runs from the Produce completion after the {@code sent} list
 * is fixed. Each item carries its own producer buffer lease; lease release is idempotent, so every
 * path that finishes an item closes it.
 */
final class BatchAccumulator {
    final Protocol.TopicPartition partition;
    final Protocol.AckMode ack;
    final List<Item> records = new ArrayList<>();
    final List<Item> sent = new ArrayList<>();
    int bytes;
    boolean usesNullRouting;
    DeadlineScheduler.Ticket linger;

    BatchAccumulator(Protocol.TopicPartition partition, Protocol.AckMode ack) {
        this.partition = partition;
        this.ack = ack;
    }

    void add(
            LogRecord record,
            CompletableFuture<Producer.RecordMetadata> result,
            ResourceBudget.Lease lease,
            long deadline,
            int encodedBytes) {
        records.add(new Item(record, result, lease, deadline));
        bytes += encodedBytes;
    }

    /**
     * Returns the records to put on the wire and remembers them in {@code sent}. Records whose
     * future is already done (for example cancelled) are dropped, and records past their client
     * deadline fail with {@link ClientException.Outcome#NOT_SENT}; both release their lease. Call
     * at most once, just before sending.
     */
    List<LogRecord> liveRecords(long now) {
        var live = new ArrayList<LogRecord>();
        for (var item : records) {
            if (item.result.isDone()) {
                item.lease.close();
                continue;
            }
            if (now >= item.deadline) {
                item.result.completeExceptionally(
                        ClientException.notSent("Record expired before send"));
                item.lease.close();
            } else {
                live.add(item.record);
                sent.add(item);
            }
        }
        return live;
    }

    /**
     * Completes every sent record from the batch outcome. On success record {@code i} receives
     * {@code firstOffset + i}. A transport or client {@code error} is passed through unchanged; an
     * entry error from the broker becomes {@link ClientException.Outcome#UNKNOWN} with its code.
     *
     * @param reply the partition result, or {@code null} when {@code error} is set
     */
    void complete(Protocol.ProduceResult reply, Throwable error) {
        long offset = reply == null ? -1 : reply.firstOffset();
        for (var item : sent) {
            item.lease.close();
            // A record completed elsewhere (cancel or close timeout) was still in the wire batch,
            // so it still consumes an offset.
            if (item.result.isDone()) {
                offset++;
                continue;
            }
            if (error != null) item.result.completeExceptionally(error);
            else if (reply.error().code() != vn.huyqt.logbroker.protocol.ErrorCode.NONE)
                item.result.completeExceptionally(
                        new ClientException(
                                ClientException.Outcome.UNKNOWN,
                                reply.error().code(),
                                reply.error().message(),
                                null));
            else item.result.complete(new Producer.RecordMetadata(partition, offset++));
        }
    }

    record Item(
            LogRecord record,
            CompletableFuture<Producer.RecordMetadata> result,
            ResourceBudget.Lease lease,
            long deadline) {}
}

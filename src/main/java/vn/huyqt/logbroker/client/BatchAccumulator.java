package vn.huyqt.logbroker.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;

/** One sealed wire batch and its individual record completions. */
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

    void add(LogRecord record, CompletableFuture<Producer.RecordMetadata> result,
            ResourceBudget.Lease lease, long deadline, int encodedBytes) {
        records.add(new Item(record, result, lease, deadline));
        bytes += encodedBytes;
    }

    List<LogRecord> liveRecords(long now) {
        var live = new ArrayList<LogRecord>();
        for (var item : records) {
            if (item.result.isDone()) {
                item.lease.close();
                continue;
            }
            if (now >= item.deadline) {
                item.result.completeExceptionally(ClientException.notSent("Record expired before send"));
                item.lease.close();
            } else {
                live.add(item.record);
                sent.add(item);
            }
        }
        return live;
    }

    void complete(Protocol.ProduceResult reply, Throwable error) {
        long offset = reply == null ? -1 : reply.firstOffset();
        for (var item : sent) {
            item.lease.close();
            if (item.result.isDone()) {
                offset++;
                continue;
            }
            if (error != null)
                item.result.completeExceptionally(error);
            else if (reply.error().code() != vn.huyqt.logbroker.protocol.ErrorCode.NONE)
                item.result.completeExceptionally(new ClientException(
                        ClientException.Outcome.UNKNOWN, reply.error().code(),
                        reply.error().message(), null));
            else
                item.result.complete(new Producer.RecordMetadata(partition, offset++));
        }
    }

    record Item(LogRecord record, CompletableFuture<Producer.RecordMetadata> result,
            ResourceBudget.Lease lease, long deadline) {
    }
}

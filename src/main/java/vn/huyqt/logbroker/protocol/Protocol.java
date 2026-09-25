package vn.huyqt.logbroker.protocol;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import vn.huyqt.logbroker.storage.LogRecord;

/** Transport-neutral version 1 request and response values. */
public final class Protocol {
    private Protocol() {}

    public record TopicPartition(UUID topicId, int partition) {
        public TopicPartition { Objects.requireNonNull(topicId); }
    }

    public enum AckMode { APPENDED, FLUSHED }

    public record Batch(List<LogRecord> records) {
        public Batch { records = List.copyOf(records); }
    }

    public record FetchBatch(long baseOffset, Batch batch) {
        public FetchBatch { Objects.requireNonNull(batch); }
    }

    public record ProduceEntry(TopicPartition partition, Batch batch) {
        public ProduceEntry { Objects.requireNonNull(partition); Objects.requireNonNull(batch); }
    }

    public record FetchEntry(TopicPartition partition, long offset, int maxBytes) {
        public FetchEntry { Objects.requireNonNull(partition); }
    }

    public record Error(ErrorCode code, String message) {
        public Error { Objects.requireNonNull(code); Objects.requireNonNull(message); }
        public static Error none() { return new Error(ErrorCode.NONE, ""); }
    }

    public record ProduceResult(TopicPartition partition, Error error,
                                long firstOffset, long nextOffset) {
        public ProduceResult { Objects.requireNonNull(partition); Objects.requireNonNull(error); }
    }

    public record FetchResult(TopicPartition partition, Error error, long logStartOffset,
                              long logEndOffset, List<FetchBatch> batches) {
        public FetchResult {
            Objects.requireNonNull(partition); Objects.requireNonNull(error);
            batches = List.copyOf(batches);
        }
    }

    public record PartitionInfo(int partition, Error error) {
        public PartitionInfo { Objects.requireNonNull(error); }
    }

    public record TopicInfo(String name, UUID id, List<PartitionInfo> partitions) {
        public TopicInfo {
            Objects.requireNonNull(name); Objects.requireNonNull(id);
            partitions = List.copyOf(partitions);
        }
    }

    public sealed interface Request permits CreateTopic, Metadata, Produce, Fetch {}
    public sealed interface Response permits CreateTopicReply, MetadataReply,
            ProduceReply, FetchReply, Failure {}

    public record CreateTopic(String name, int partitions) implements Request {
        public CreateTopic { Objects.requireNonNull(name); }
    }

    public record Metadata(List<String> names) implements Request {
        public Metadata { names = List.copyOf(names); }
    }

    public record Produce(AckMode ack, int timeoutMs, List<ProduceEntry> entries)
            implements Request {
        public Produce { Objects.requireNonNull(ack); entries = List.copyOf(entries); }
    }

    public record Fetch(int maxBytes, int minBytes, int maxWaitMs,
                        List<FetchEntry> entries) implements Request {
        public Fetch { entries = List.copyOf(entries); }
    }

    public record CreateTopicReply(Error error, UUID topicId) implements Response {
        public CreateTopicReply { Objects.requireNonNull(error); Objects.requireNonNull(topicId); }
    }

    public record MetadataReply(Error error, String host, int port,
                                List<TopicInfo> topics) implements Response {
        public MetadataReply {
            Objects.requireNonNull(error); Objects.requireNonNull(host);
            topics = List.copyOf(topics);
        }
    }

    public record ProduceReply(Error error, List<ProduceResult> results) implements Response {
        public ProduceReply { Objects.requireNonNull(error); results = List.copyOf(results); }
    }

    public record FetchReply(Error error, List<FetchResult> results) implements Response {
        public FetchReply { Objects.requireNonNull(error); results = List.copyOf(results); }
    }

    public record Failure(Error error) implements Response {
        public Failure { Objects.requireNonNull(error); }
    }

    public record RequestFrame(short operation, short version, long requestId, Request body) {
        public RequestFrame { Objects.requireNonNull(body); }
    }

    public record ResponseFrame(short operation, short version, long requestId, Response body) {
        public ResponseFrame { Objects.requireNonNull(body); }
    }
}

package vn.huyqt.logbroker.protocol;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import vn.huyqt.logbroker.storage.LogRecord;

/** Transport-neutral version 1 request and response values. */
public final class Protocol {
    private Protocol() {
    }

    /** Partition reference: topic UUID plus partition ID, never the topic name. */
    public record TopicPartition(UUID topicId, int partition) {
        public TopicPartition {
            Objects.requireNonNull(topicId);
        }
    }

    /**
     * Produce acknowledgment. {@code APPENDED} (wire 0) confirms the local append; {@code FLUSHED}
     * (wire 1) confirms the local durable end covers the batch. Neither implies replication.
     */
    public enum AckMode {
        APPENDED, FLUSHED
    }

    /** Records of one wire batch. Offsets are not part of it; the broker assigns them. */
    public record Batch(List<LogRecord> records) {
        public Batch {
            records = List.copyOf(records);
        }
    }

    /**
     * A fetched batch and the offset of its first record, which may precede the requested offset.
     */
    public record FetchBatch(long baseOffset, Batch batch) {
        public FetchBatch {
            Objects.requireNonNull(batch);
        }
    }

    /** One partition's batch in a Produce request; at most one entry per partition. */
    public record ProduceEntry(TopicPartition partition, Batch batch) {
        public ProduceEntry {
            Objects.requireNonNull(partition);
            Objects.requireNonNull(batch);
        }
    }

    /** One partition in a Fetch request; {@code maxBytes} is the per-partition budget. */
    public record FetchEntry(TopicPartition partition, long offset, int maxBytes) {
        public FetchEntry {
            Objects.requireNonNull(partition);
        }
    }

    /**
     * Error scope of a response or of one entry. The message is diagnostic text capped at 512
     * UTF-8 bytes on the wire; {@link ErrorCode#NONE} is expected to carry an empty message.
     */
    public record Error(ErrorCode code, String message) {
        public Error {
            Objects.requireNonNull(code);
            Objects.requireNonNull(message);
        }

        /** Returns the success value, {@code NONE} with an empty message. */
        public static Error none() {
            return new Error(ErrorCode.NONE, "");
        }
    }

    /**
     * Outcome for one Produce entry. On success {@code nextOffset} is exclusive; both offsets are
     * -1 when {@code error} is not {@code NONE}.
     */
    public record ProduceResult(TopicPartition partition, Error error,
            long firstOffset, long nextOffset) {
        public ProduceResult {
            Objects.requireNonNull(partition);
            Objects.requireNonNull(error);
        }
    }

    /**
     * Outcome for one Fetch entry. {@code logEndOffset} is exclusive and may include unflushed
     * data; both offsets are -1 when {@code error} is not {@code NONE}. An empty batch list is a
     * success and does not mean the end of the log was reached.
     */
    public record FetchResult(TopicPartition partition, Error error, long logStartOffset,
            long logEndOffset, List<FetchBatch> batches) {
        public FetchResult {
            Objects.requireNonNull(partition);
            Objects.requireNonNull(error);
            batches = List.copyOf(batches);
        }
    }

    /** Availability of one partition in a Metadata response. */
    public record PartitionInfo(int partition, Error error) {
        public PartitionInfo {
            Objects.requireNonNull(error);
        }
    }

    public record TopicInfo(String name, UUID id, List<PartitionInfo> partitions) {
        public TopicInfo {
            Objects.requireNonNull(name);
            Objects.requireNonNull(id);
            partitions = List.copyOf(partitions);
        }
    }

    /** Request body; the frame's operation ID must match the body type. */
    public sealed interface Request permits CreateTopic, Metadata, Produce, Fetch,
            ClusterProtocol.CreateTopic, ClusterProtocol.Metadata, ClusterProtocol.Produce, ClusterProtocol.Fetch {
    }

    /**
     * Response body. A nonzero top-level error is encoded without the body and always decodes as
     * {@link Failure}.
     */
    public sealed interface Response permits CreateTopicReply, MetadataReply,
            ProduceReply, FetchReply, Failure, ClusterProtocol.CreateTopicReply, ClusterProtocol.MetadataReply,
            ClusterProtocol.ProduceReply, ClusterProtocol.FetchReply {
    }

    public record CreateTopic(String name, int partitions) implements Request {
        public CreateTopic {
            Objects.requireNonNull(name);
        }
    }

    /** Metadata request; an empty name list asks for all topics. */
    public record Metadata(List<String> names) implements Request {
        public Metadata {
            names = List.copyOf(names);
        }
    }

    /**
     * Produce request. Entries of different partitions are not appended atomically, and a retry
     * after a lost response may append a duplicate.
     */
    public record Produce(AckMode ack, int timeoutMs, List<ProduceEntry> entries)
            implements Request {
        public Produce {
            Objects.requireNonNull(ack);
            entries = List.copyOf(entries);
        }
    }

    /**
     * Fetch request. {@code maxBytes} caps the whole response's batch bytes, {@code minBytes} is
     * the amount to wait for, and {@code maxWaitMs} bounds the wait.
     */
    public record Fetch(int maxBytes, int minBytes, int maxWaitMs,
            List<FetchEntry> entries) implements Request {
        public Fetch {
            entries = List.copyOf(entries);
        }
    }

    public record CreateTopicReply(Error error, UUID topicId) implements Response {
        public CreateTopicReply {
            Objects.requireNonNull(error);
            Objects.requireNonNull(topicId);
        }
    }

    public record MetadataReply(Error error, String host, int port,
            List<TopicInfo> topics) implements Response {
        public MetadataReply {
            Objects.requireNonNull(error);
            Objects.requireNonNull(host);
            topics = List.copyOf(topics);
        }
    }

    public record ProduceReply(Error error, List<ProduceResult> results) implements Response {
        public ProduceReply {
            Objects.requireNonNull(error);
            results = List.copyOf(results);
        }
    }

    public record FetchReply(Error error, List<FetchResult> results) implements Response {
        public FetchReply {
            Objects.requireNonNull(error);
            results = List.copyOf(results);
        }
    }

    /** A response whose top-level error is not {@code NONE}; it carries no body. */
    public record Failure(Error error) implements Response {
        public Failure {
            Objects.requireNonNull(error);
        }
    }

    /** A request envelope; {@code requestId} only correlates and never deduplicates writes. */
    public record RequestFrame(short operation, short version, long requestId, Request body) {
        public RequestFrame {
            Objects.requireNonNull(body);
        }
    }

    /** A response envelope echoing the request's operation, version and ID. */
    public record ResponseFrame(short operation, short version, long requestId, Response body) {
        public ResponseFrame {
            Objects.requireNonNull(body);
        }
    }
}

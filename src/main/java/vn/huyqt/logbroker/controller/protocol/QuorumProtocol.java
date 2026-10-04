package vn.huyqt.logbroker.controller.protocol;

import java.util.*;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.log.QuorumBatch;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

/**
 * Controller protocol v1 message model. Field order and widths follow {@code
 * docs/controller-protocol-v1.md}; {@link QuorumCodec} does the encoding and range checks. Array
 * and list components are copied on construction, and arrays again on access, so a message can be
 * shared between threads without its bytes changing.
 */
public final class QuorumProtocol {
    private QuorumProtocol() {}

    /**
     * Where to send the reply to one inbound request: the connection incarnation and the request
     * ID. Closing the connection invalidates its routes; a new socket reusing the same request ID
     * does not revive them.
     */
    public record ReplyRoute(long connectionId, long requestId) {}

    public sealed interface Message permits Request, Reply {}

    public sealed interface Request extends Message
            permits Vote,
                    BeginQuorumEpoch,
                    EndQuorumEpoch,
                    QuorumFetch,
                    FetchSnapshot,
                    DescribeQuorum,
                    CreateTopic,
                    ReadMetadata,
                    ReadLocalMetadata,
                    BrokerControlProtocol.Register,
                    BrokerControlProtocol.Heartbeat,
                    BrokerControlProtocol.ObserverFetch,
                    BrokerControlProtocol.ObserverSnapshot,
                    BrokerControlProtocol.CreateTopic {}

    public sealed interface Reply extends Message
            permits VoteReply,
                    EpochReply,
                    QuorumFetchReply,
                    FetchSnapshotReply,
                    DescribeQuorumReply,
                    CreateTopicReply,
                    MetadataReply,
                    Failure,
                    BrokerControlProtocol.RegisterReply,
                    BrokerControlProtocol.HeartbeatReply,
                    BrokerControlProtocol.ObserverFetchReply,
                    BrokerControlProtocol.ObserverSnapshotReply,
                    BrokerControlProtocol.CreateTopicReply,
                    BrokerControlProtocol.DescribeReply,
                    BrokerControlProtocol.MetadataReply {
        /**
         * Error, message, epoch and leader hint; any error other than {@code NONE} ends the body.
         */
        ReplyMeta meta();
    }

    /**
     * One decoded frame: the fixed envelope plus its message.
     *
     * @param operation operation number, which is the same in a request and its response
     * @param senderId voter ID of the sending node, or -1 for an admin client
     * @param requestId non-negative; correlates a response within one connection incarnation
     * @param voterHash {@link vn.huyqt.logbroker.controller.ClusterIdentity#voterHash()} of the
     *     sender's membership; always 32 bytes
     */
    public record Frame(
            short version,
            BrokerControlProtocol.SenderRole senderRole,
            short operation,
            boolean response,
            UUID clusterId,
            int senderId,
            long requestId,
            byte[] voterHash,
            Message message) {
        public Frame {
            Objects.requireNonNull(clusterId);
            Objects.requireNonNull(message);
            Objects.requireNonNull(senderRole);
            if (version != 1 && version != 2)
                throw new IllegalArgumentException("Unsupported frame schema");
            if (voterHash.length != 32 || requestId < 0 || senderId < -1)
                throw new IllegalArgumentException("Invalid frame identity");
            voterHash = voterHash.clone();
        }

        /** Legacy envelope; v1 infers voter/admin role from senderId. */
        public Frame(
                short operation,
                boolean response,
                UUID clusterId,
                int senderId,
                long requestId,
                byte[] voterHash,
                Message message) {
            this(
                    (short) 1,
                    senderId == -1
                            ? BrokerControlProtocol.SenderRole.ADMIN
                            : BrokerControlProtocol.SenderRole.VOTER,
                    operation,
                    response,
                    clusterId,
                    senderId,
                    requestId,
                    voterHash,
                    message);
        }

        @Override
        public byte[] voterHash() {
            return voterHash.clone();
        }
    }

    /**
     * Reply prefix. {@code epoch} and {@code leaderId} are the sender's current knowledge, with -1
     * for an unknown leader; the hint does not prove leadership. The message is at most 512 UTF-8
     * bytes when decoded.
     */
    public record ReplyMeta(QuorumError error, String message, long epoch, int leaderId) {
        public ReplyMeta {
            Objects.requireNonNull(error);
            Objects.requireNonNull(message);
        }
    }

    /**
     * Vote request from a candidate.
     *
     * @param epoch the candidate's new epoch
     * @param lastEpoch epoch of the candidate's last durable log entry
     * @param end exclusive end offset of the candidate's durable log
     */
    public record Vote(long epoch, long lastEpoch, long end) implements Request {}

    public record BeginQuorumEpoch(long epoch) implements Request {}

    public record EndQuorumEpoch(long epoch) implements Request {}

    /**
     * Follower fetch from the leader.
     *
     * @param end exclusive end of the follower's flushed log, where fetching resumes
     * @param lastEpoch epoch of the entry just before {@code end}
     * @param maxBytes response data budget, at most the configured fetch maximum
     * @param maxWaitMs how long the leader may park a caught-up fetch waiting for new data; 0 asks
     *     for an immediate reply, and the value is at most the fetch idle wait
     * @param challenge the nonce from the leader's previous fetch reply; only an echo of a recent
     *     nonce counts as fresh contact for the leader
     */
    public record QuorumFetch(
            long epoch, long end, long lastEpoch, int maxBytes, int maxWaitMs, long challenge)
            implements Request {}

    /**
     * Requests one chunk of an immutable snapshot.
     *
     * @param position byte offset of the chunk within the snapshot file
     * @param maxBytes chunk size limit, at most the configured snapshot chunk size
     */
    public record FetchSnapshot(long epoch, SnapshotId id, long position, int maxBytes)
            implements Request {}

    public record DescribeQuorum() implements Request {}

    /**
     * Admin topic creation, idempotent for an identical name and partition count.
     *
     * @param timeoutMs how long the controller may wait for the result, at most the admin timeout
     */
    public record CreateTopic(String name, int partitions, int timeoutMs) implements Request {}

    /**
     * Admin linearizable read, answered after a fresh read barrier commits and applies.
     *
     * @param timeoutMs as for {@link CreateTopic}
     */
    public record ReadMetadata(int timeoutMs) implements Request {}

    public record ReadLocalMetadata() implements Request {}

    public record VoteReply(ReplyMeta meta, boolean granted) implements Reply {}

    public record EpochReply(ReplyMeta meta) implements Reply {}

    /** What a successful fetch reply carries after its challenge and commit fields. */
    public sealed interface FetchPayload permits FetchData, Divergence, SnapshotRequired {}

    /**
     * Contiguous whole leader batches starting at the fetch position; empty when there is no new
     * data. Followers append them without regrouping.
     */
    public record FetchData(List<QuorumBatch> batches) implements FetchPayload {
        public FetchData {
            batches = List.copyOf(batches);
        }
    }

    /**
     * The fetch position does not match the leader's log. The follower uses the reported epoch and
     * end to find the common prefix and truncates its divergent uncommitted suffix.
     */
    public record Divergence(long commonEpoch, long commonEnd) implements FetchPayload {}

    /** The needed prefix is no longer in the leader's log; the follower installs {@code id}. */
    public record SnapshotRequired(SnapshotId id) implements FetchPayload {}

    /**
     * Leader reply to {@link QuorumFetch}.
     *
     * @param challenge new nonce the follower must echo in its next fetch
     * @param commit the leader's exclusive commit offset
     */
    public record QuorumFetchReply(
            ReplyMeta meta, long challenge, long commit, FetchPayload payload) implements Reply {}

    /**
     * One snapshot chunk. On the wire it is followed by a CRC32C of the chunk, which the decoder
     * verifies; the frame CRC does not replace it.
     *
     * @param position byte offset of {@code chunk} within the snapshot
     * @param totalLength size of the whole snapshot file
     */
    public record FetchSnapshotReply(
            ReplyMeta meta, SnapshotId id, long position, long totalLength, byte[] chunk)
            implements Reply {
        public FetchSnapshotReply {
            chunk = chunk.clone();
        }

        /** Chunk size without copying the chunk. */
        public int chunkLength() {
            return chunk.length;
        }

        @Override
        public byte[] chunk() {
            return chunk.clone();
        }
    }

    public record DescribeQuorumReply(ReplyMeta meta, QuorumStatus status) implements Reply {}

    public record CreateTopicReply(ReplyMeta meta, UUID topicId) implements Reply {}

    /** Read guarantee of a {@link MetadataView}. */
    public enum Consistency {
        // Encoded by ordinal (LOCAL=0, LINEARIZABLE=1): reordering changes the wire format.
        LOCAL,
        LINEARIZABLE
    }

    /**
     * Catalog snapshot returned by metadata reads. A {@code LOCAL} view is this node's applied
     * state and may be stale; a {@code LINEARIZABLE} view was captured after a fresh barrier
     * committed and applied on the leader.
     *
     * @param leaderId known leader, or -1
     * @param commit exclusive commit offset known to the node
     * @param applied exclusive applied offset; never above {@code commit}
     */
    public record MetadataView(
            Consistency consistency,
            int nodeId,
            long epoch,
            int leaderId,
            long commit,
            long applied,
            List<TopicCreated> topics) {
        public MetadataView {
            topics = List.copyOf(topics);
        }
    }

    public record MetadataReply(ReplyMeta meta, MetadataView view) implements Reply {}

    /** Reply of any operation whose error is not {@code NONE}; the body ends after the prefix. */
    public record Failure(ReplyMeta meta) implements Reply {}

    /** Returns the v1 operation number, 101 to 109, used for both the request and its response. */
    public static short operation(Request request) {
        return switch (request) {
            case Vote ignored -> 101;
            case BeginQuorumEpoch ignored -> 102;
            case EndQuorumEpoch ignored -> 103;
            case QuorumFetch ignored -> 104;
            case FetchSnapshot ignored -> 105;
            case DescribeQuorum ignored -> 106;
            case CreateTopic ignored -> 107;
            case ReadMetadata ignored -> 108;
            case ReadLocalMetadata ignored -> 109;
            case BrokerControlProtocol.CreateTopic ignored -> 107;
            case BrokerControlProtocol.Register ignored -> 110;
            case BrokerControlProtocol.Heartbeat ignored -> 111;
            case BrokerControlProtocol.ObserverFetch ignored -> 112;
            case BrokerControlProtocol.ObserverSnapshot ignored -> 113;
        };
    }
}

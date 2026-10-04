package vn.huyqt.logbroker.controller.consensus;

import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

import java.util.*;

/**
 * Input to {@link QuorumStateMachine#on}, delivered one at a time by the controller event loop.
 * Offsets are exclusive batch boundaries.
 */
public sealed interface QuorumEvent {
    /** Monotonic clock tick that drives timeouts, elections and fetch scheduling. */
    record Tick(long nowNanos) implements QuorumEvent {}

    /** Request from a peer voter, answered on {@code route}. */
    record PeerRequest(Frame frame, ReplyRoute route) implements QuorumEvent {}

    /** Response from a peer voter; ignored unless it correlates with an outstanding request. */
    record PeerResponse(Frame frame) implements QuorumEvent {}

    /**
     * Identifies one disk operation. The epoch and generation it was issued under let the state
     * machine discard completions that no longer apply; the operation ID keeps tokens unique.
     */
    record DiskToken(long operationId, long epoch, UUID generation) {}

    /** Outcome of a {@link QuorumEffect.DiskEffect}. */
    sealed interface DiskResult {
        /** The vote is durable; carries the stored epoch and vote. */
        record VoteSaved(long epoch, int votedFor) implements DiskResult {}

        /** A batch was written; {@code durableEnd} may lag the new log end until a flush. */
        record Appended(QuorumBatch batch, EpochIndex index, long durableEnd)
                implements DiskResult {}

        /** The log was forced; {@code end} is the new durable end. */
        record Flushed(long end) implements DiskResult {}

        /** The suffix was removed; {@code index} describes the remaining log. */
        record Truncated(EpochIndex index) implements DiskResult {}

        /** The commit checkpoint at {@code end} is durable. */
        record Checkpointed(long end) implements DiskResult {}

        /** Batches read for a {@link QuorumEffect.ReadLog}. */
        record Read(List<QuorumBatch> batches) implements DiskResult {
            public Read {
                batches = List.copyOf(batches);
            }
        }

        /**
         * A snapshot install published {@code generation}; the log now starts at the snapshot end.
         */
        record Installed(UUID generation, SnapshotId id, EpochIndex index, MetadataImage image)
                implements DiskResult {}

        record DownloadStarted(SnapshotId id) implements DiskResult {}

        /** A chunk was written; {@code end} is the number of bytes downloaded so far. */
        record ChunkWritten(SnapshotId id, long end) implements DiskResult {}

        /** The download was validated and published; carries its decoded image. */
        record DownloadFinished(SnapshotId id, MetadataImage image) implements DiskResult {}

        /** Chunk read for upload to a peer. */
        record SnapshotChunk(SnapshotId id, long position, long totalLength, byte[] bytes)
                implements DiskResult {
            // Copies keep the chunk immutable after it leaves the disk worker for the event loop.
            public SnapshotChunk {
                bytes = bytes.clone();
            }

            @Override
            public byte[] bytes() {
                return bytes.clone();
            }
        }

        /** The requested snapshot cannot be served; {@code error} is returned to the peer. */
        record SnapshotRejected(vn.huyqt.logbroker.controller.protocol.QuorumError error)
                implements DiskResult {}

        /**
         * The operation had no effect: it belonged to an inactive generation, the install fence
         * closed, or it was a download cancellation.
         */
        record Discarded() implements DiskResult {}

        /** The operation never ran because disk or read-memory admission was full. */
        record Overloaded() implements DiskResult {}

        record SnapshotCreated(SnapshotId id) implements DiskResult {}

        /** Prefix retention finished; {@code base} is the new recovery base snapshot. */
        record PrefixRetained(EpochIndex index, SnapshotId base) implements DiskResult {}
    }

    /** A disk operation completed with {@code result}. */
    record DiskDone(DiskToken token, DiskResult result) implements QuorumEvent {}

    /** A disk operation threw; fails the node unless the token is stale. */
    record DiskFailed(DiskToken token, String failure) implements QuorumEvent {}

    /**
     * Committed batches were applied up to {@code end}; ignored if {@code generation} is set and no
     * longer active.
     */
    record Applied(long end, MetadataImage image, UUID generation) implements QuorumEvent {
        public Applied(long end, MetadataImage image) {
            this(end, image, null);
        }
    }

    /** Begins shutdown: cancels transfers and fails pending admin requests. */
    record Stop() implements QuorumEvent {}

    /** Leader-only proposal; ignored unless this node is a ready leader. */
    record Propose(List<QuorumEntry> entries) implements QuorumEvent {
        public Propose {
            entries = List.copyOf(entries);
        }
    }

    /** A new snapshot was published and can be offered to followers that need one. */
    record SnapshotAvailable(SnapshotId id) implements QuorumEvent {}

    /** Admin request with an absolute monotonic deadline. */
    record Admin(long invocationId, Request request, long deadlineNanos) implements QuorumEvent {}

    /** Self-scheduled event that appends queued topic creations and a barrier for queued reads. */
    record DrainProposals() implements QuorumEvent {}

    /** The log prefix before {@code base} was retired; {@code index} describes the retained log. */
    record LogRetained(EpochIndex index, SnapshotId base) implements QuorumEvent {}

    /** Inbound transport message, handled by the controller node before reaching consensus. */
    record Network(vn.huyqt.logbroker.controller.transport.QuorumTransport.Inbound inbound)
            implements QuorumEvent {}

    /** Runs {@code action} on the event loop; handled by the controller node. */
    record Invoke(Runnable action) implements QuorumEvent {}

    /** Unrecoverable runtime failure; fails the node. */
    record Fatal(String failure) implements QuorumEvent {}
}

package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.DiskToken;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

/**
 * Side effect requested by {@link QuorumStateMachine#on}; the caller interprets it outside the
 * state machine. Offsets are exclusive batch boundaries.
 */
public sealed interface QuorumEffect {
  /**
   * Work for the ordered disk worker. Its outcome returns as a {@link QuorumEvent.DiskDone} or
   * {@link QuorumEvent.DiskFailed} carrying the same token.
   */
  sealed interface DiskEffect extends QuorumEffect {
    DiskToken token();
  }

  /**
   * Durably records the epoch and vote ({@code voter} -1 for none). Not tied to a generation,
   * because votes are global hard state.
   */
  record PersistVote(DiskToken token, long epoch, int voter) implements DiskEffect {}

  /** Leader append of {@code entries} as one batch at the log end; not durable until flushed. */
  record Append(DiskToken token, long epoch, List<QuorumEntry> entries) implements DiskEffect {
    public Append {
      entries = List.copyOf(entries);
    }
  }

  /** Follower append of a leader batch with its offsets unchanged; not durable until flushed. */
  record AppendReplica(DiskToken token, QuorumBatch batch) implements DiskEffect {}

  /** Forces the log; completes with the new durable end. */
  record Flush(DiskToken token) implements DiskEffect {}

  /** Removes the uncommitted suffix after {@code end} during reconciliation. */
  record Truncate(DiskToken token, long end) implements DiskEffect {}

  /** Durably checkpoints {@code end} as committed; may lag the in-memory commit. */
  record Checkpoint(DiskToken token, long end) implements DiskEffect {}

  /** Reads whole batches from {@code offset} within {@code budget} bytes. */
  record ReadLog(DiskToken token, long offset, int budget) implements DiskEffect {}

  /** Observer read stamped with its current session and frozen committed upper bound. */
  record ReadObserver(DiskToken token, vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session session,
                      long offset, long upperBound, int budget) implements DiskEffect {}

  record ReadObserverSnapshot(DiskToken token, int brokerId, SnapshotId id, long position, int maxBytes)
      implements DiskEffect {}

  /** Releases expired upload pins, or all observer pins when the controller epoch closes. */
  record MaintainUploads(DiskToken token, boolean closeObservers) implements DiskEffect {}

  /** Publishes a downloaded snapshot as a new generation, if the install fence still allows. */
  record InstallSnapshot(DiskToken token, SnapshotId id, MetadataImage image)
      implements DiskEffect {}

  /** Starts a snapshot download, replacing any partial one. */
  record BeginDownload(DiskToken token, SnapshotId id) implements DiskEffect {}

  /** Writes one received chunk at {@code position} of the partial download. */
  record WriteSnapshotChunk(DiskToken token, SnapshotId id, long position, byte[] bytes)
      implements DiskEffect {
    // Copies keep the chunk immutable after it leaves the event loop for the disk worker.
    public WriteSnapshotChunk {
      bytes = bytes.clone();
    }

    @Override
    public byte[] bytes() {
      return bytes.clone();
    }
  }

  /** Validates and publishes a complete download of {@code totalLength} bytes. */
  record FinishDownload(DiskToken token, SnapshotId id, long totalLength) implements DiskEffect {}

  /** Discards the partial download, if any. */
  record CancelDownload(DiskToken token) implements DiskEffect {}

  /** Leader-side read of one snapshot chunk for upload to {@code peer}. */
  record ReadSnapshotChunk(DiskToken token, int peer, SnapshotId id, long position, int maxBytes)
      implements DiskEffect {}

  /** Replaces the applied metadata image after a snapshot install. */
  record Restore(MetadataImage image) implements QuorumEffect {}

  /** Writes a snapshot of an applied image whose last included entry has {@code lastEpoch}. */
  record CreateSnapshot(DiskToken token, MetadataImage image, long lastEpoch)
      implements DiskEffect {}

  /**
   * Makes the older of the two retained snapshots, ending at {@code olderEnd}, the recovery base
   * and deletes the sealed segments before it; see {@code docs/controller-storage-v1.md}.
   */
  record RetainSnapshotPrefix(DiskToken token, long olderEnd) implements DiskEffect {}

  /** Sends a request to a peer voter. */
  record Send(int peerId, Frame frame) implements QuorumEffect {}

  /** Replies on the connection route the request arrived on. */
  record Reply(ReplyRoute route, Frame frame) implements QuorumEffect {}

  /**
   * Applies committed batches to the metadata state machine; ignored if {@code generation} is set
   * and no longer active.
   */
  record Apply(List<QuorumBatch> batches, UUID generation) implements QuorumEffect {
    public Apply {
      batches = List.copyOf(batches);
    }

    public Apply(List<QuorumBatch> batches) {
      this(batches, null);
    }
  }

  /** The node has failed and stopped participating in the quorum. */
  record Fail(String failure) implements QuorumEffect {}

  /** Completes the admin invocation {@code invocationId} with {@code reply}. */
  record CompleteAdmin(
      long invocationId, vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Reply reply)
      implements QuorumEffect {}

  /** Feeds {@code event} back into the state machine as a later event. */
  record Enqueue(QuorumEvent event) implements QuorumEffect {}
}

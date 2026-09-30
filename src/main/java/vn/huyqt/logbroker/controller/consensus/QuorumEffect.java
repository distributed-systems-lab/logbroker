package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.DiskToken;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

public sealed interface QuorumEffect {
  sealed interface DiskEffect extends QuorumEffect {
    DiskToken token();
  }

  record PersistVote(DiskToken token, long epoch, int voter) implements DiskEffect {}

  record Append(DiskToken token, long epoch, List<QuorumEntry> entries) implements DiskEffect {
    public Append {
      entries = List.copyOf(entries);
    }
  }

  record AppendReplica(DiskToken token, QuorumBatch batch) implements DiskEffect {}

  record Flush(DiskToken token) implements DiskEffect {}

  record Truncate(DiskToken token, long end) implements DiskEffect {}

  record Checkpoint(DiskToken token, long end) implements DiskEffect {}

  record ReadLog(DiskToken token, long offset, int budget) implements DiskEffect {}

  record InstallSnapshot(DiskToken token, SnapshotId id, MetadataImage image)
      implements DiskEffect {}

  record BeginDownload(DiskToken token, SnapshotId id) implements DiskEffect {}

  record WriteSnapshotChunk(DiskToken token, SnapshotId id, long position, byte[] bytes)
      implements DiskEffect {
    public WriteSnapshotChunk {
      bytes = bytes.clone();
    }

    @Override
    public byte[] bytes() {
      return bytes.clone();
    }
  }

  record FinishDownload(DiskToken token, SnapshotId id, long totalLength) implements DiskEffect {}

  record CancelDownload(DiskToken token) implements DiskEffect {}

  record ReadSnapshotChunk(DiskToken token, int peer, SnapshotId id, long position, int maxBytes)
      implements DiskEffect {}

  record Restore(MetadataImage image) implements QuorumEffect {}

  record CreateSnapshot(DiskToken token, MetadataImage image, long lastEpoch)
      implements DiskEffect {}

  record RetainSnapshotPrefix(DiskToken token, long olderEnd) implements DiskEffect {}

  record Send(int peerId, Frame frame) implements QuorumEffect {}

  record Reply(ReplyRoute route, Frame frame) implements QuorumEffect {}

  record Apply(List<QuorumBatch> batches, UUID generation) implements QuorumEffect {
    public Apply {
      batches = List.copyOf(batches);
    }

    public Apply(List<QuorumBatch> batches) {
      this(batches, null);
    }
  }

  record Fail(String failure) implements QuorumEffect {}

  record CompleteAdmin(
      long invocationId, vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Reply reply)
      implements QuorumEffect {}

  record Enqueue(QuorumEvent event) implements QuorumEffect {}
}

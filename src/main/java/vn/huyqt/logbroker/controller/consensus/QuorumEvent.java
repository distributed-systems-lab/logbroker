package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

public sealed interface QuorumEvent {
    record Tick(long nowNanos) implements QuorumEvent {}
    record PeerRequest(Frame frame,ReplyRoute route) implements QuorumEvent {}
    record PeerResponse(Frame frame) implements QuorumEvent {}
    record DiskToken(long operationId,long epoch,UUID generation) {}
    sealed interface DiskResult {
        record VoteSaved(long epoch,int votedFor) implements DiskResult {}
        record Appended(QuorumBatch batch,EpochIndex index,long durableEnd) implements DiskResult {}
        record Flushed(long end) implements DiskResult {}
        record Truncated(EpochIndex index) implements DiskResult {}
        record Checkpointed(long end) implements DiskResult {}
        record Read(List<QuorumBatch> batches) implements DiskResult {public Read{batches=List.copyOf(batches);}}
        record Installed(UUID generation,SnapshotId id,EpochIndex index,MetadataImage image) implements DiskResult {}
        record DownloadStarted(SnapshotId id) implements DiskResult {}
        record ChunkWritten(SnapshotId id,long end) implements DiskResult {}
        record DownloadFinished(SnapshotId id,MetadataImage image) implements DiskResult {}
        record SnapshotChunk(SnapshotId id,long position,long totalLength,byte[] bytes) implements DiskResult {public SnapshotChunk{bytes=bytes.clone();}@Override public byte[] bytes(){return bytes.clone();}}
        record SnapshotRejected(vn.huyqt.logbroker.controller.protocol.QuorumError error) implements DiskResult {}
        record Discarded() implements DiskResult {}
        record SnapshotCreated(SnapshotId id) implements DiskResult {}
        record PrefixRetained(EpochIndex index,SnapshotId base) implements DiskResult {}
    }
    record DiskDone(DiskToken token,DiskResult result) implements QuorumEvent {}
    record DiskFailed(DiskToken token,String failure) implements QuorumEvent {}
    record Applied(long end,MetadataImage image,UUID generation) implements QuorumEvent {public Applied(long end,MetadataImage image){this(end,image,null);}}
    record Stop() implements QuorumEvent {}
    record Propose(List<QuorumEntry> entries) implements QuorumEvent {public Propose{entries=List.copyOf(entries);}}
    record SnapshotAvailable(SnapshotId id) implements QuorumEvent {}
    record Admin(long invocationId,Request request,long deadlineNanos) implements QuorumEvent {}
    record DrainProposals() implements QuorumEvent {}
    record LogRetained(EpochIndex index,SnapshotId base) implements QuorumEvent {}
}

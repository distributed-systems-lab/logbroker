package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.DiskToken;

public sealed interface QuorumEffect {
    sealed interface DiskEffect extends QuorumEffect { DiskToken token(); }
    record PersistVote(DiskToken token,long epoch,int voter) implements DiskEffect {}
    record Append(DiskToken token,long epoch,List<QuorumEntry> entries) implements DiskEffect {public Append{entries=List.copyOf(entries);}}
    record AppendReplica(DiskToken token,QuorumBatch batch) implements DiskEffect {}
    record Flush(DiskToken token) implements DiskEffect {}
    record Truncate(DiskToken token,long end) implements DiskEffect {}
    record Checkpoint(DiskToken token,long end) implements DiskEffect {}
    record ReadLog(DiskToken token,long offset,int budget) implements DiskEffect {}
    record InstallSnapshot(DiskToken token,SnapshotId id) implements DiskEffect {}
    record Send(int peerId,Frame frame) implements QuorumEffect {}
    record Reply(ReplyRoute route,Frame frame) implements QuorumEffect {}
    record Apply(List<QuorumBatch> batches) implements QuorumEffect {public Apply{batches=List.copyOf(batches);}}
    record Fail(String failure) implements QuorumEffect {}
    record CompleteAdmin(long invocationId,vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Reply reply) implements QuorumEffect {}
    record Enqueue(QuorumEvent event) implements QuorumEffect {}
}

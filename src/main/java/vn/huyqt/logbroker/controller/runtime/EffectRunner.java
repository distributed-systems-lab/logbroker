package vn.huyqt.logbroker.controller.runtime;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.metadata.MetadataStateMachine;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotStore;

/** Executes disk work off-loop, but applies committed metadata on the state-owning loop. */
public final class EffectRunner {
    private final QuorumStateStore state;private final GenerationStore generation;private final MetadataStateMachine metadata;
    private final OrderedDiskExecutor disk;private final Consumer<QuorumEvent> events;private final Consumer<QuorumEffect> external;
    private SnapshotStore snapshots;
    public EffectRunner(QuorumStateStore state,GenerationStore generation,MetadataStateMachine metadata,OrderedDiskExecutor disk,
            Consumer<QuorumEvent> events,Consumer<QuorumEffect> external){this.state=state;this.generation=generation;this.metadata=metadata;this.disk=disk;this.events=events;this.external=external;}
    public void snapshots(SnapshotStore snapshots){this.snapshots=snapshots;}
    public void run(List<QuorumEffect> effects){for(var effect:effects){
        if(effect instanceof QuorumEffect.DiskEffect work){boolean accepted=work instanceof QuorumEffect.CreateSnapshot?disk.submitLowPriority(work.token(),()->execute(work)):disk.submit(work.token(),()->execute(work));if(!accepted)events.accept(new DiskFailed(work.token(),"Disk admission exhausted"));}
        else if(effect instanceof QuorumEffect.Apply apply){try{for(var batch:apply.batches())metadata.apply(batch);events.accept(new Applied(metadata.image().appliedOffset(),metadata.image()));}catch(IOException e){external.accept(new QuorumEffect.Fail(e.toString()));}}
        else if(effect instanceof QuorumEffect.Enqueue enqueue)events.accept(enqueue.event());
        else external.accept(effect);
    }}
    private DiskResult execute(QuorumEffect.DiskEffect effect)throws IOException {
        return switch(effect){
            case QuorumEffect.PersistVote vote->{state.persistVote(vote.epoch(),vote.voter());yield new DiskResult.VoteSaved(state.epoch(),state.votedFor());}
            case QuorumEffect.Append append->{var batch=generation.log().append(append.epoch(),append.entries());yield new DiskResult.Appended(batch,generation.log().epochs(),generation.log().durableEnd());}
            case QuorumEffect.AppendReplica append->{generation.log().appendReplica(append.batch());yield new DiskResult.Appended(append.batch(),generation.log().epochs(),generation.log().durableEnd());}
            case QuorumEffect.Flush ignored->new DiskResult.Flushed(generation.log().flush());
            case QuorumEffect.Truncate truncate->{generation.truncate(truncate.end());yield new DiskResult.Truncated(generation.log().epochs());}
            case QuorumEffect.Checkpoint checkpoint->{generation.checkpointCommit(checkpoint.end());yield new DiskResult.Checkpointed(checkpoint.end());}
            case QuorumEffect.ReadLog read->new DiskResult.Read(generation.log().read(read.offset(),read.budget()));
            case QuorumEffect.InstallSnapshot ignored->throw new IOException("Snapshot installer is not connected");
            case QuorumEffect.CreateSnapshot create->{if(snapshots==null)throw new IOException("Snapshot store not connected");yield new DiskResult.SnapshotCreated(snapshots.create(create.image(),create.lastEpoch()));}
            case QuorumEffect.RetainSnapshotPrefix retain->{generation.retainPrefix(retain.olderEnd());snapshots.releaseObsolete();yield new DiskResult.PrefixRetained(generation.epochIndex(),generation.baseSnapshot());}
        };
    }
}

package vn.huyqt.logbroker.controller.support;

import java.io.IOException;
import java.util.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.log.*;

/** Controllable disk completions; volatile and forced prefixes are intentionally distinct. */
public final class FakeDisk {
    private final ArrayDeque<QuorumEffect.DiskEffect> pending=new ArrayDeque<>();
    private List<QuorumBatch> batches=new ArrayList<>(),forced=new ArrayList<>();
    private long epoch,committed;private int vote=-1;private boolean failForce;
    public void enqueue(QuorumEffect.DiskEffect work){if(pending.size()>=256)throw new AssertionError("Fake disk queue unbounded");pending.addLast(work);}
    public int pending(){return pending.size();}
    public void failNextForce(){failForce=true;}
    public long epoch(){return epoch;}
    public int votedFor(){return vote;}
    public long committed(){return committed;}
    public EpochIndex index(){return new EpochIndex(0,0,batches);}
    public long durable(){return forced.isEmpty()?0:forced.getLast().nextOffset();}
    public List<QuorumBatch> batches(){return List.copyOf(batches);}
    public QuorumEvent completeNext(){
        var work=pending.removeFirst();
        try{return new DiskDone(work.token(),execute(work));}
        catch(Exception e){return new DiskFailed(work.token(),e.toString());}
    }
    private DiskResult execute(QuorumEffect.DiskEffect work)throws IOException {
        return switch(work){
            case QuorumEffect.PersistVote p->{forceBoundary();if(p.epoch()<epoch||p.epoch()==epoch&&vote!=-1&&vote!=p.voter())throw new IOException("Double vote");epoch=p.epoch();vote=p.voter();yield new DiskResult.VoteSaved(epoch,vote);}
            case QuorumEffect.Append p->{var batch=new QuorumBatch(index().end(),p.entries());batches.add(batch);yield new DiskResult.Appended(batch,index(),durable());}
            case QuorumEffect.AppendReplica p->{if(p.batch().baseOffset()!=index().end())throw new IOException("Append gap");batches.add(p.batch());yield new DiskResult.Appended(p.batch(),index(),durable());}
            case QuorumEffect.Flush ignored->{forceBoundary();forced=new ArrayList<>(batches);yield new DiskResult.Flushed(durable());}
            case QuorumEffect.Truncate p->{if(p.end()<committed)throw new IOException("Truncate committed data");index().positionAt(p.end());batches=new ArrayList<>(batches.stream().filter(b->b.nextOffset()<=p.end()).toList());forced=new ArrayList<>(batches);yield new DiskResult.Truncated(index());}
            case QuorumEffect.Checkpoint p->{forceBoundary();if(p.end()>durable()||p.end()<committed)throw new IOException("Invalid commit checkpoint");committed=p.end();yield new DiskResult.Checkpointed(committed);}
            case QuorumEffect.ReadLog p->{var result=new ArrayList<QuorumBatch>();long bytes=0;for(var batch:batches){if(batch.nextOffset()<=p.offset())continue;long size=16;for(var e:batch.entries())size+=4+QuorumEntryCodec.encode(e).length;if(!result.isEmpty()&&bytes+size>p.budget())break;result.add(batch);bytes+=size;if(bytes>=p.budget())break;}yield new DiskResult.Read(result);}
            case QuorumEffect.InstallSnapshot ignored->throw new IOException("Fake snapshot install not connected");
            case QuorumEffect.CreateSnapshot create->{forceBoundary();yield new DiskResult.SnapshotCreated(new vn.huyqt.logbroker.controller.snapshot.SnapshotId(create.image().appliedOffset(),create.lastEpoch(),UUID.randomUUID()));}
            case QuorumEffect.RetainSnapshotPrefix retain->throw new IOException("Fake retention not connected");
        };
    }
    private void forceBoundary()throws IOException{if(failForce){failForce=false;throw new IOException("Injected force failure");}}
    public int pendingSnapshots(){return (int)pending.stream().filter(QuorumEffect.CreateSnapshot.class::isInstance).count();}
    public void powerLoss(){batches=new ArrayList<>(forced);pending.clear();}
    public void processCrash(){pending.clear();}
    public void recover(){forced=new ArrayList<>(batches);}
}

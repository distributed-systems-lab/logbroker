package vn.huyqt.logbroker.controller.support;

import java.time.Duration;
import java.util.*;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.metadata.MetadataStateMachine;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/** Production transitions with controlled disk/network/time; no role is forced by the harness. */
public final class QuorumHarness {
    private final QuorumStateMachine[] nodes=new QuorumStateMachine[3];private final FakeDisk[] disks=new FakeDisk[3];
    private final MetadataStateMachine[] metadata=new MetadataStateMachine[3];private final boolean[] paused=new boolean[3];
    private final ControllerService[] services=new ControllerService[3];
    private final ArrayDeque<Runnable> responses=new ArrayDeque<>();
    private final ArrayDeque<Runnable> reentries=new ArrayDeque<>();
    private final SimulatedTransport transport=new SimulatedTransport();private final long seed;private long now;
    private final int adminCapacity;
    private final Map<Long,Map<Long,QuorumEntryEvidence>> committedEvidence=new HashMap<>();
    private record QuorumEntryEvidence(Object entry){}
    private QuorumHarness(long seed,int adminCapacity){this.seed=seed;this.adminCapacity=adminCapacity;for(int i=0;i<3;i++){disks[i]=new FakeDisk(new UUID(0,i+10));restart(i);}}
    public static QuorumHarness threeNodes(long seed){return new QuorumHarness(seed,1024);}
    public static QuorumHarness threeNodes(long seed,int adminCapacity){return new QuorumHarness(seed,adminCapacity);}
    public QuorumStateMachine node(int node){return nodes[node];}
    public FakeDisk disk(int node){return disks[node];}
    public ControllerService service(int node){return services[node];}
    public SimulatedTransport transport(){return transport;}
    public long now(){return now;}
    public void tickNode(int node,Duration duration){now+=duration.toNanos();deliver(node,new QuorumEvent.Tick(now));}
    public void request(int node,Frame frame){deliver(node,new QuorumEvent.PeerRequest(frame,new ReplyRoute(frame.senderId(),frame.requestId())));}
    public void appendBarrier(int node){deliver(node,new QuorumEvent.Propose(List.of(new vn.huyqt.logbroker.controller.log.QuorumEntry.ReadBarrier(nodes[node].status().epoch()))));}
    public int leader(){for(int i=0;i<3;i++)if(nodes[i]!=null&&nodes[i].status().role()==QuorumStatus.Role.LEADER)return i;return -1;}
    public void elect(int node){for(int attempts=0;attempts<20;attempts++){now+=Duration.ofSeconds(4).toNanos();deliver(node,new QuorumEvent.Tick(now));settle();if(leader()==node)return;}throw new AssertionError("Target node did not win election");}
    public void tick(Duration duration){now+=duration.toNanos();for(int i=0;i<3;i++)if(nodes[i]!=null)deliver(i,new QuorumEvent.Tick(now));}
    private void deliver(int node,QuorumEvent event){
        if(nodes[node]==null)return;
        for(var effect:nodes[node].on(event)){
            if(effect instanceof QuorumEffect.DiskEffect work)disks[node].enqueue(work);
            else if(effect instanceof QuorumEffect.Send send)transport.send(node,send);
            else if(effect instanceof QuorumEffect.Reply reply)transport.reply(node,reply);
            else if(effect instanceof QuorumEffect.Apply apply){try{for(var batch:apply.batches())metadata[node].apply(batch);deliver(node,new QuorumEvent.Applied(metadata[node].image().appliedOffset(),metadata[node].image(),apply.generation()));}catch(Exception e){throw new AssertionError(e);}}
            else if(effect instanceof QuorumEffect.Fail failure){if(nodes[node].status().role()!=QuorumStatus.Role.FAILED)throw new AssertionError(failure.failure());}
            else if(effect instanceof QuorumEffect.CompleteAdmin complete)services[node].complete(complete.invocationId(),complete.reply());
            else if(effect instanceof QuorumEffect.Enqueue enqueue)reentries.addLast(()->deliver(node,enqueue.event()));
            else if(effect instanceof QuorumEffect.Restore restore){try{metadata[node].restore(restore.image());deliver(node,new QuorumEvent.Applied(restore.image().appliedOffset(),restore.image()));}catch(Exception e){throw new AssertionError(e);}}
        }
        assertSafety();
    }
    public void deliverAll(){SimulatedTransport.Envelope envelope;int count=0;while((envelope=transport.next())!=null){if(++count>10000)throw new AssertionError("Network livelock");
        deliver(envelope.target(),envelope.frame().response()?new QuorumEvent.PeerResponse(envelope.frame()):new QuorumEvent.PeerRequest(envelope.frame(),envelope.route()));}}
    public void completeDisks(){for(int i=0;i<3;i++)if(nodes[i]!=null&&!paused[i]){int count=disks[i].pending();for(int j=0;j<count;j++)deliver(i,disks[i].completeNext());}}
    public void completeOneDisk(int node){deliver(node,disks[node].completeNext());}
    public void settle(){for(int steps=0;steps<10000;steps++){int pending=transport.pending()+responses.size()+reentries.size();for(int i=0;i<3;i++)if(nodes[i]!=null&&!paused[i])pending+=disks[i].pending();if(pending==0)return;while(!reentries.isEmpty())reentries.removeFirst().run();completeDisks();deliverAll();while(!responses.isEmpty())responses.removeFirst().run();}throw new AssertionError("Quorum failed to settle");}
    public void isolate(int node){transport.isolate(node);}
    public void heal(){transport.heal();}
    public void pauseDisk(int node){paused[node]=true;}
    public void resumeDisk(int node){paused[node]=false;}
    public void crash(int node){nodes[node]=null;disks[node].processCrash();}
    public void powerLoss(int node){crash(node);disks[node].powerLoss();}
    public void restart(int node){
        disks[node].recover();metadata[node]=new MetadataStateMachine();
        try{metadata[node].restore(disks[node].installedImage());}catch(Exception e){throw new AssertionError(e);}
        try{for(var batch:disks[node].batches())if(batch.nextOffset()<=disks[node].committed())metadata[node].apply(batch);}catch(Exception e){throw new AssertionError(e);}
        var index=disks[node].index();var status=new QuorumStatus(node,QuorumStatus.Role.UNATTACHED,disks[node].epoch(),-1,disks[node].generation(),index.end(),disks[node].durable(),disks[node].committed(),metadata[node].image().appliedOffset(),index.start(),false,Map.of(),"");
        nodes[node]=new QuorumStateMachine(ControllerConfig.builder(ControllerTestSupport.identity(node)).maxPendingRequests(adminCapacity).build(),status,index,disks[node].votedFor(),new Random(seed+node),now,()->this.now,metadata[node].image());
        services[node]=new ControllerService(adminCapacity,event->{deliver(node,event);return true;},responses::addLast,()->this.now);
    }
    public void compact(int node){try{var id=disks[node].compact(metadata[node].image());deliver(node,new QuorumEvent.SnapshotAvailable(id));deliver(node,new QuorumEvent.LogRetained(disks[node].index(),id));}catch(Exception e){throw new AssertionError(e);}}
    public void assertSafety(){
        var leaders=new HashMap<Long,Integer>();
        for(int i=0;i<3;i++)if(nodes[i]!=null){var s=nodes[i].status();
            if(s.snapshotEnd()>s.applied()||s.applied()>s.commit()||s.commit()>s.durableEnd()||s.durableEnd()>s.logEnd())throw new AssertionError("Invalid offset progress: "+s);
            if(s.role()==QuorumStatus.Role.LEADER){Integer other=leaders.put(s.epoch(),i);if(other!=null)throw new AssertionError("Two leaders in epoch "+s.epoch());}
            var evidence=committedEvidence.computeIfAbsent(0L,ignored->new HashMap<>());
            for(var batch:disks[i].batches())if(batch.nextOffset()<=s.commit())for(int j=0;j<batch.entries().size();j++){
                long offset=batch.baseOffset()+j;var value=new QuorumEntryEvidence(batch.entries().get(j));var old=evidence.putIfAbsent(offset,value);
                if(old!=null&&!old.equals(value))throw new AssertionError("Committed prefix changed at "+offset);
            }
        }
    }
}

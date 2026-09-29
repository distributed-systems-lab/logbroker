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
    private final vn.huyqt.logbroker.storage.LogConfig logConfig;
    private final Map<Long,Map<Long,QuorumEntryEvidence>> committedEvidence=new HashMap<>();
    private record QuorumEntryEvidence(Object entry){}
    private final List<HistoryEvent> history=new ArrayList<>();private long invocationIds;
    private final Map<String,vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated> acknowledged=new HashMap<>();
    private final Map<String,Integer> voteEvidence=new HashMap<>();
    public List<HistoryEvent> history(){return List.copyOf(history);}
    public void recordAcknowledgedForTest(String name,UUID id,int partitions){acknowledged.put(name,new vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated(id,name,partitions));}
    public java.util.concurrent.CompletableFuture<UUID> trackedCreate(int node,String name,int partitions){long id=++invocationIds;history.add(new HistoryEvent.Invocation(id,new HistoryEvent.Create(name,partitions),now));var result=services[node].createTopic(name,partitions,now+Duration.ofSeconds(2).toNanos());result.whenComplete((uuid,error)->{if(error==null){recordAcknowledgedForTest(name,uuid,partitions);history.add(new HistoryEvent.Completion(id,new HistoryEvent.Created(uuid),now));}else recordFailure(id,error);});return result;}
    private void trackedRead(int node){long id=++invocationIds;history.add(new HistoryEvent.Invocation(id,new HistoryEvent.Read(),now));services[node].readMetadata(now+Duration.ofSeconds(2).toNanos()).whenComplete((view,error)->{if(error==null)history.add(new HistoryEvent.Completion(id,new HistoryEvent.Topics(view.topics()),now));else recordFailure(id,error);});}
    private void recordFailure(long id,Throwable error){Throwable cause=error instanceof java.util.concurrent.CompletionException?error.getCause():error;var code=cause instanceof ControllerService.ServiceException e?e.meta().error():vn.huyqt.logbroker.controller.protocol.QuorumError.NODE_UNAVAILABLE;history.add(new HistoryEvent.Completion(id,(code==vn.huyqt.logbroker.controller.protocol.QuorumError.REQUEST_TIMED_OUT||code==vn.huyqt.logbroker.controller.protocol.QuorumError.NODE_UNAVAILABLE||code==vn.huyqt.logbroker.controller.protocol.QuorumError.NOT_LEADER)?new HistoryEvent.Unknown():new HistoryEvent.Rejected(code),now));}
    public void runSchedule(long scheduleSeed,int steps){var random=new Random(scheduleSeed);var trace=new ArrayDeque<String>();try{elect(0);for(int step=0;step<steps;step++){int target=random.nextInt(3),action=random.nextInt(12);trace.addLast(step+":"+action+":"+target);if(trace.size()>80)trace.removeFirst();switch(action){
        case 0->{tick(Duration.ofMillis(20+random.nextInt(80)));}
        case 1->{transport.dropNext();}
        case 2->{transport.reorder();deliverOne();}
        case 3->{if(nodes[target]!=null&&!paused[target]&&disks[target].pending()>0)completeOneDisk(target);}
        case 4->{isolate(target);}
        case 5->{heal();}
        case 6->{if(nodes[target]!=null){if(random.nextBoolean())powerLoss(target);else crash(target);}else{paused[target]=false;restart(target);}}
        case 7->{paused[target]=!paused[target];}
        case 8->{int leader=leader();if(leader>=0&&node(leader).status().ready()&&invocationIds<8){if(invocationIds%3==2)trackedRead(leader);else trackedCreate(leader,"topic-"+(invocationIds%4),1);}}
        case 9->{if(nodes[target]!=null&&disks[target].pending()==0&&metadata[target].image().appliedOffset()>disks[target].index().start())compact(target);}
        default->{deliverOne();}
    }while(!reentries.isEmpty())reentries.removeFirst().run();while(!responses.isEmpty())responses.removeFirst().run();assertSafety();}runHealthySuffix();}catch(Throwable failure){throw new AssertionError("seed="+scheduleSeed+" trace="+trace,failure);}}
    private void deliverOne(){var envelope=transport.next();if(envelope!=null){history.add(new HistoryEvent.Delivery(envelope.source(),envelope.target(),envelope.frame().requestId()));deliver(envelope.target(),envelope.frame().response()?new QuorumEvent.PeerResponse(envelope.frame()):new QuorumEvent.PeerRequest(envelope.frame(),envelope.route()));}}
    public void runHealthySuffix(){heal();for(int i=0;i<3;i++){paused[i]=false;if(nodes[i]==null)restart(i);}for(int i=0;i<800;i++){tick(Duration.ofMillis(20));settle();}if(leader()<0||!nodes[leader()].status().ready())throw new AssertionError("No ready leader after healthy suffix");}
    public void assertAcknowledgedTopicsSurvive(){runHealthySuffix();for(int i=0;i<3;i++){var actual=new HashMap<String,vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated>();for(var topic:metadata[i].image().topics())actual.put(topic.name(),topic);for(var topic:acknowledged.values())if(!topic.equals(actual.get(topic.name())))throw new AssertionError("Acknowledged topic lost on node "+i+": "+topic);}}
    private QuorumHarness(long seed,int adminCapacity){this(seed,adminCapacity,vn.huyqt.logbroker.storage.LogConfig.defaults());}
    private QuorumHarness(long seed,int adminCapacity,vn.huyqt.logbroker.storage.LogConfig logConfig){this.seed=seed;this.adminCapacity=adminCapacity;this.logConfig=logConfig;for(int i=0;i<3;i++){disks[i]=new FakeDisk(new UUID(0,i+10));restart(i);}}
    public static QuorumHarness threeNodes(long seed,int capacity,vn.huyqt.logbroker.storage.LogConfig log){return new QuorumHarness(seed,capacity,log);}
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
    public void crash(int node){history.add(new HistoryEvent.Crash(node));services[node].close();nodes[node]=null;disks[node].processCrash();}
    public void powerLoss(int node){crash(node);disks[node].powerLoss();}
    public void restart(int node){
        disks[node].recover();metadata[node]=new MetadataStateMachine();
        try{metadata[node].restore(disks[node].installedImage());}catch(Exception e){throw new AssertionError(e);}
        try{for(var batch:disks[node].batches())if(batch.nextOffset()<=disks[node].committed())metadata[node].apply(batch);}catch(Exception e){throw new AssertionError(e);}
        var index=disks[node].index();var status=new QuorumStatus(node,QuorumStatus.Role.UNATTACHED,disks[node].epoch(),-1,disks[node].generation(),index.end(),disks[node].durable(),disks[node].committed(),metadata[node].image().appliedOffset(),index.start(),false,Map.of(),"");
        nodes[node]=new QuorumStateMachine(ControllerConfig.builder(ControllerTestSupport.identity(node)).logConfig(logConfig).maxPendingRequests(adminCapacity).build(),status,index,disks[node].votedFor(),new Random(seed+node),now,()->this.now,metadata[node].image());
        services[node]=new ControllerService(adminCapacity,event->{deliver(node,event);return true;},responses::addLast,()->this.now);
    }
    public void compact(int node){try{var id=disks[node].compact(metadata[node].image());deliver(node,new QuorumEvent.SnapshotAvailable(id));deliver(node,new QuorumEvent.LogRetained(disks[node].index(),id));}catch(Exception e){throw new AssertionError(e);}}
    public void assertSafety(){
        for(int i=0;i<3;i++)if(disks[i]!=null&&disks[i].votedFor()>=0){String key=i+":"+disks[i].epoch();Integer old=voteEvidence.putIfAbsent(key,disks[i].votedFor());if(old!=null&&old!=disks[i].votedFor())throw new AssertionError("Vote changed after restart: "+key);}
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

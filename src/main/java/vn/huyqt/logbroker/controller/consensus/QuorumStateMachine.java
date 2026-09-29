package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import java.util.random.RandomGenerator;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

/** Pure transitions; I/O effects are interpreted outside this state owner. */
public final class QuorumStateMachine {
    private final ControllerConfig config;private QuorumStatus status;private EpochIndex index;private int votedFor;
    private final RandomGenerator random;
    private final LongSupplier clock;
    private long persistedEpoch,now,electionDeadline,operationId,requestId;
    private final Map<DiskToken,Consumer<DiskResult>> diskCallbacks=new HashMap<>();
    private final Set<DiskToken> logWork=new HashSet<>();
    private final Map<Long,Integer> voteRequests=new HashMap<>();
    private final Set<Integer> votes=new HashSet<>();
    private List<QuorumEffect> effects;
    private boolean electionPending;
    private final ReplicationTracker replication=new ReplicationTracker();
    private record WaitingFetch(PeerRequest request,long deadline){}
    private record Challenge(long nonce,long issuedAt){}
    private record FetchFlight(long requestId,int leader,long epoch,UUID generation,long deadline){}
    private final Map<Integer,WaitingFetch> waitingFetches=new HashMap<>();
    private final Set<Integer> fetchReads=new HashSet<>();
    private final Map<Integer,Challenge> challenges=new HashMap<>();
    private final Map<Integer,Long> contacts=new HashMap<>();
    private FetchFlight fetchFlight;
    private long nonce,echoChallenge,nextFetchAt,leaderSince,leaderMarkerEnd,leaderCommit;
    private boolean replicaBusy,applyBusy;
    private SnapshotId snapshot;
    public QuorumStateMachine(ControllerConfig config,QuorumStatus status,EpochIndex index,int votedFor,RandomGenerator random,long now) {
        this(config,status,index,votedFor,random,now,()->now);
    }
    public QuorumStateMachine(ControllerConfig config,QuorumStatus status,EpochIndex index,int votedFor,RandomGenerator random,long now,LongSupplier clock) {
        this.config=config;this.status=status;this.index=index;this.votedFor=votedFor;this.random=random;
        this.clock=clock;
        this.now=now;persistedEpoch=status.epoch();resetElectionDeadline();
    }
    public List<QuorumEffect> on(QuorumEvent event) {
        now=Math.max(now,clock.getAsLong());
        effects=new ArrayList<>();
        if(event instanceof DiskDone done) {
            var callback=diskCallbacks.remove(done.token());logWork.remove(done.token());
            if(callback!=null&&done.token().generation().equals(status.generation())&&active()) {
                updatePhysical(done.result());callback.accept(done.result());
            }
        } else if(event instanceof DiskFailed failed) {
            diskCallbacks.remove(failed.token());logWork.remove(failed.token());
            if(failed.token().generation().equals(status.generation())&&active())fail(failed.failure());
        } else if(event instanceof Stop) transition(QuorumStatus.Role.STOPPING,status.epoch(),-1,false,"");
        else if(active()) {
            if(event instanceof Tick tick) {
                now=Math.max(now,tick.nowNanos());
                if(status.role()==QuorumStatus.Role.LEADER) {
                    long last=contacts.values().stream().mapToLong(Long::longValue).max().orElse(leaderSince);
                    if(now-last>=config.leaderContactTimeout().toNanos())stepDown(status.epoch(),-1);
                    else {
                        for(var pending:List.copyOf(waitingFetches.values()))if(now>=pending.deadline())serveFetch(pending.request());
                    }
                }
                if(fetchFlight!=null&&now>=fetchFlight.deadline()){fetchFlight=null;nextFetchAt=now;}
                if(status.role()==QuorumStatus.Role.FOLLOWER&&status.leaderId()>=0&&now>=nextFetchAt)fetch();
                if(status.role()!=QuorumStatus.Role.LEADER&&now>=electionDeadline&&!electionPending)prepareElection();
            } else if(event instanceof PeerRequest request&&validPeer(request.frame()))handleRequest(request);
            else if(event instanceof PeerResponse response&&validPeer(response.frame()))handleResponse(response.frame());
            else if(event instanceof Propose proposal&&status.role()==QuorumStatus.Role.LEADER&&status.ready())append(proposal.entries(),false);
            else if(event instanceof Applied applied) {
                if(applied.end()<status.applied()||applied.end()>status.commit())fail("Invalid applied boundary");
                else {
                    progress(status.logEnd(),status.durableEnd(),status.commit(),applied.end());applyBusy=false;
                    if(status.role()==QuorumStatus.Role.LEADER&&leaderMarkerEnd>0&&status.applied()>=leaderMarkerEnd)
                        transition(status.role(),status.epoch(),status.leaderId(),true,"");
                    applyCommitted();
                }
            } else if(event instanceof SnapshotAvailable available)snapshot=available.id();
        }
        return List.copyOf(effects);
    }
    private boolean active(){return status.role()!=QuorumStatus.Role.FAILED&&status.role()!=QuorumStatus.Role.STOPPING;}
    private boolean validPeer(Frame frame){return frame.senderId()!=status.nodeId()&&config.identity().voters().stream().anyMatch(v->v.id()==frame.senderId())
        &&frame.clusterId().equals(config.identity().clusterId())&&Arrays.equals(frame.voterHash(),config.identity().voterHash());}
    private void prepareElection() {
        electionPending=true;
        if(!logWork.isEmpty()||status.durableEnd()<status.logEnd()) {
            var token=token();diskCallbacks.put(token,result->{electionPending=false;if(now>=electionDeadline)startElection();});
            effects.add(new QuorumEffect.Flush(token));
        } else {electionPending=false;startElection();}
    }
    private void startElection() {
        long epoch=Math.addExact(status.epoch(),1);
        transition(QuorumStatus.Role.CANDIDATE,epoch,-1,false,"");votedFor=status.nodeId();
        votes.clear();voteRequests.clear();resetElectionDeadline();
        persistVote(epoch,votedFor,()->{
            if(status.epoch()!=epoch||status.role()!=QuorumStatus.Role.CANDIDATE)return;
            votes.add(status.nodeId());var position=index.positionAt(status.durableEnd());
            for(var voter:config.identity().voters())if(voter.id()!=status.nodeId()) {
                var frame=request(new Vote(epoch,position.lastEpoch(),position.endOffset()));
                voteRequests.put(frame.requestId(),voter.id());effects.add(new QuorumEffect.Send(voter.id(),frame));
            }
        });
    }
    private void handleRequest(PeerRequest event) {
        Request request=(Request)event.frame().message();
        long epoch=switch(request){case Vote v->v.epoch();case BeginQuorumEpoch b->b.epoch();case EndQuorumEpoch e->e.epoch();case QuorumFetch f->f.epoch();case FetchSnapshot f->f.epoch();default->status.epoch();};
        if(epoch<status.epoch()){if(persistedEpoch==status.epoch())reply(event,errorReply(request,QuorumError.STALE_EPOCH));return;}
        if(epoch>status.epoch()) {
            stepDown(epoch,-1);votedFor=-1;
            persistVote(epoch,-1,()->{if(status.epoch()==epoch)handleRequest(event);});return;
        }
        if(request instanceof Vote vote) {
            if(status.durableEnd()<status.logEnd()||!logWork.isEmpty()) {
                var token=token();diskCallbacks.put(token,result->{if(status.epoch()==vote.epoch())handleRequest(event);});
                effects.add(new QuorumEffect.Flush(token));return;
            }
            var local=index.positionAt(status.durableEnd());
            boolean fresh=vote.lastEpoch()>local.lastEpoch()||vote.lastEpoch()==local.lastEpoch()&&vote.end()>=local.endOffset();
            boolean eligible=fresh&&(votedFor==-1||votedFor==event.frame().senderId())&&status.leaderId()<0;
            if(!eligible){if(persistedEpoch==status.epoch())reply(event,new VoteReply(meta(QuorumError.NONE),false));return;}
            votedFor=event.frame().senderId();resetElectionDeadline();
            persistVote(vote.epoch(),votedFor,()->{if(status.epoch()==vote.epoch()&&votedFor==event.frame().senderId())reply(event,new VoteReply(meta(QuorumError.NONE),true));});
        } else if(request instanceof BeginQuorumEpoch begin) {
            if(status.leaderId()!=-1&&status.leaderId()!=event.frame().senderId()||status.role()==QuorumStatus.Role.LEADER) {
                if(persistedEpoch==status.epoch())reply(event,new EpochReply(meta(QuorumError.INVALID_REQUEST)));return;
            }
            stepDown(begin.epoch(),event.frame().senderId());resetElectionDeadline();
            persistVote(begin.epoch(),votedFor,()->{if(status.epoch()==begin.epoch()&&status.leaderId()==event.frame().senderId()) {reply(event,new EpochReply(meta(QuorumError.NONE)));fetch();}});
        } else if(request instanceof EndQuorumEpoch end) {
            if(status.leaderId()==event.frame().senderId()){stepDown(end.epoch(),-1);electionDeadline=now;}
            if(persistedEpoch==status.epoch())reply(event,new EpochReply(meta(QuorumError.NONE)));
        } else if(request instanceof QuorumFetch fetch) {
            if(status.role()!=QuorumStatus.Role.LEADER||persistedEpoch!=status.epoch()){if(persistedEpoch==status.epoch())reply(event,new Failure(meta(QuorumError.NOT_LEADER)));return;}
            int peer=event.frame().senderId();
            Challenge challenge=challenges.get(peer);
            if(challenge!=null&&fetch.challenge()==challenge.nonce()&&now-challenge.issuedAt()<config.leaderContactTimeout().toNanos()) {
                contacts.put(peer,now);challenges.remove(peer);
            }
            replication.confirm(peer,fetch.end(),fetch.lastEpoch(),index);publishMatches();advanceLeaderCommit();
            if(fetchReads.contains(peer))return;
            var pending=waitingFetches.get(peer);
            if(pending!=null)return; // One admitted pull per follower, with a fixed wait deadline.
            boolean exact=matches(fetch.end(),fetch.lastEpoch());
            if(exact&&fetch.end()==status.logEnd()&&fetch.maxWaitMs()>0)
                waitingFetches.put(peer,new WaitingFetch(event,now+fetch.maxWaitMs()*1_000_000L));
            else serveFetch(event);
        }
    }
    private void handleResponse(Frame frame) {
        Reply reply=(Reply)frame.message();
        boolean correlated=voteRequests.getOrDefault(frame.requestId(),-1)==frame.senderId()
            ||fetchFlight!=null&&fetchFlight.requestId()==frame.requestId()&&fetchFlight.leader()==frame.senderId();
        if(!correlated)return;
        if(reply.meta().epoch()>status.epoch()) {
            long epoch=reply.meta().epoch();stepDown(epoch,-1);votedFor=-1;persistVote(epoch,-1,()->{});return;
        }
        if(reply instanceof VoteReply vote&&status.role()==QuorumStatus.Role.CANDIDATE&&vote.meta().epoch()==status.epoch()
            &&vote.meta().error()==QuorumError.NONE&&vote.granted()) {
            Integer expected=voteRequests.remove(frame.requestId());if(expected==null||expected!=frame.senderId())return;
            votes.add(frame.senderId());if(votes.size()>=2)becomeLeader();
        } else if(reply instanceof QuorumFetchReply fetch&&fetchFlight!=null&&status.role()==QuorumStatus.Role.FOLLOWER
            &&frame.senderId()==status.leaderId()&&fetch.meta().epoch()==status.epoch()
            &&fetchFlight.generation().equals(status.generation())) {
            fetchFlight=null;
            if(fetch.meta().error()!=QuorumError.NONE){nextFetchAt=now+config.fetchIdleWait().toNanos();return;}
            echoChallenge=fetch.challenge();resetElectionDeadline();leaderCommit=Math.max(leaderCommit,fetch.commit());
            if(fetch.payload() instanceof FetchData data) {
                if(data.batches().isEmpty()){advanceFollowerCommit();nextFetchAt=now+config.fetchIdleWait().toNanos();return;}
                long end=status.logEnd(),last=index.positionAt(end).lastEpoch();
                for(var batch:data.batches()) {
                    if(batch.baseOffset()!=end||batch.entries().getFirst().epoch()<last||batch.entries().getFirst().epoch()>status.epoch()){fail("Invalid replication sequence");return;}
                    end=batch.nextOffset();last=batch.entries().getFirst().epoch();
                }
                replicaBusy=true;long epoch=status.epoch();
                for(var batch:data.batches()) {var token=token();logWork.add(token);diskCallbacks.put(token,ignored->{});effects.add(new QuorumEffect.AppendReplica(token,batch));}
                var flush=token();logWork.add(flush);diskCallbacks.put(flush,result->{replicaBusy=false;if(status.epoch()==epoch&&status.role()==QuorumStatus.Role.FOLLOWER){advanceFollowerCommit();fetch();}});
                effects.add(new QuorumEffect.Flush(flush));
            } else if(fetch.payload() instanceof Divergence divergence) {
                reconcile(divergence);
            } else if(fetch.payload() instanceof SnapshotRequired required) {
                // Transfer admission is connected in Task 12; do not advertise progress meanwhile.
                snapshot=required.id();nextFetchAt=now+config.fetchIdleWait().toNanos();
            }
        }
    }
    private void becomeLeader() {
        transition(QuorumStatus.Role.LEADER,status.epoch(),status.nodeId(),false,"");voteRequests.clear();
        replication.reset(status.epoch());contacts.clear();challenges.clear();leaderSince=now;leaderMarkerEnd=0;
        for(var voter:config.identity().voters())if(voter.id()!=status.nodeId())effects.add(new QuorumEffect.Send(voter.id(),request(new BeginQuorumEpoch(status.epoch()))));
        append(List.of(new QuorumEntry.LeaderChange(status.epoch(),status.nodeId())),true);
    }
    private void append(List<QuorumEntry> entries,boolean marker) {
        var token=token();logWork.add(token);diskCallbacks.put(token,result->{
            if(status.role()!=QuorumStatus.Role.LEADER||status.epoch()!=token.epoch())return;
            if(marker)leaderMarkerEnd=((DiskResult.Appended)result).batch().nextOffset();
            for(var pending:List.copyOf(waitingFetches.values()))serveFetch(pending.request());
            var flush=token();logWork.add(flush);diskCallbacks.put(flush,ignored->{if(status.role()==QuorumStatus.Role.LEADER&&status.epoch()==flush.epoch())advanceLeaderCommit();});
            effects.add(new QuorumEffect.Flush(flush));
        });effects.add(new QuorumEffect.Append(token,status.epoch(),entries));
    }
    private void fetch() {
        if(status.role()!=QuorumStatus.Role.FOLLOWER||status.leaderId()<0||fetchFlight!=null||replicaBusy||!logWork.isEmpty()||persistedEpoch!=status.epoch())return;
        var position=index.positionAt(status.durableEnd());
        var frame=request(new QuorumFetch(status.epoch(),position.endOffset(),position.lastEpoch(),config.fetchMaxBytes(),(int)config.fetchIdleWait().toMillis(),echoChallenge));
        fetchFlight=new FetchFlight(frame.requestId(),status.leaderId(),status.epoch(),status.generation(),now+config.rpcTimeout().toNanos());
        effects.add(new QuorumEffect.Send(status.leaderId(),frame));
    }
    private boolean matches(long end,long epoch){try{return index.positionAt(end).lastEpoch()==epoch;}catch(IllegalArgumentException ignored){return false;}}
    private void serveFetch(PeerRequest event) {
        int peer=event.frame().senderId();waitingFetches.remove(peer);
        var fetch=(QuorumFetch)event.frame().message();
        if(status.role()!=QuorumStatus.Role.LEADER||status.epoch()!=fetch.epoch())return;
        if(!matches(fetch.end(),fetch.lastEpoch())) {
            try {var common=index.commonPrefix(fetch.end(),fetch.lastEpoch());sendFetch(event,new Divergence(common.lastEpoch(),common.endOffset()));}
            catch(IllegalArgumentException missing){if(snapshot!=null)sendFetch(event,new SnapshotRequired(snapshot));else reply(event,new Failure(meta(QuorumError.NODE_UNAVAILABLE)));}
            return;
        }
        var token=token();fetchReads.add(peer);diskCallbacks.put(token,result->{
            fetchReads.remove(peer);
            if(status.role()==QuorumStatus.Role.LEADER&&status.epoch()==token.epoch())sendFetch(event,new FetchData(((DiskResult.Read)result).batches()));
        });effects.add(new QuorumEffect.ReadLog(token,fetch.end(),fetch.maxBytes()));
    }
    private void sendFetch(PeerRequest event,FetchPayload payload) {
        long issued=++nonce;challenges.put(event.frame().senderId(),new Challenge(issued,now));
        reply(event,new QuorumFetchReply(meta(QuorumError.NONE),issued,status.commit(),payload));
    }
    private void reconcile(Divergence divergence) {
        long common;
        try {common=Math.min(divergence.commonEnd(),index.endOfEpoch(divergence.commonEpoch()));}
        catch(IllegalArgumentException missing) {
            common=index.boundaries().stream().filter(p->p.lastEpoch()<divergence.commonEpoch()).mapToLong(EpochIndex.LogPosition::endOffset).max().orElse(-1);
        }
        if(common<status.commit()||common>=status.logEnd()||common<index.start()) {fail("Divergence cannot safely advance reconciliation");return;}
        if(!matches(common,index.positionAt(common).lastEpoch())){fail("Invalid divergence boundary");return;}
        replicaBusy=true;var token=token();logWork.add(token);
        diskCallbacks.put(token,result->{replicaBusy=false;if(status.epoch()==token.epoch()&&status.role()==QuorumStatus.Role.FOLLOWER)fetch();});
        effects.add(new QuorumEffect.Truncate(token,common));
    }
    private void publishMatches(){status=new QuorumStatus(status.nodeId(),status.role(),status.epoch(),status.leaderId(),status.generation(),status.logEnd(),status.durableEnd(),status.commit(),status.applied(),status.snapshotEnd(),status.ready(),replication.matches(),status.failure());}
    private void advanceLeaderCommit(){advanceCommit(replication.committable(status.durableEnd(),status.epoch(),index));}
    private void advanceFollowerCommit(){
        long ceiling=Math.min(leaderCommit,status.durableEnd());
        long end=index.boundaries().stream().filter(p->p.endOffset()<=ceiling).mapToLong(EpochIndex.LogPosition::endOffset).max().orElse(status.commit());advanceCommit(end);
    }
    private void advanceCommit(long end) {
        if(end<=status.commit())return;
        progress(status.logEnd(),status.durableEnd(),end,status.applied());
        var token=token();diskCallbacks.put(token,ignored->{});effects.add(new QuorumEffect.Checkpoint(token,end));applyCommitted();
    }
    private void applyCommitted() {
        if(applyBusy||status.applied()>=status.commit())return;
        applyBusy=true;long target=status.commit(),from=status.applied();var token=token();
        diskCallbacks.put(token,result->{
            var batches=((DiskResult.Read)result).batches().stream().filter(b->b.baseOffset()>=from&&b.nextOffset()<=target).toList();
            if(batches.isEmpty()){fail("Committed batch missing during apply");return;}effects.add(new QuorumEffect.Apply(batches));
        });effects.add(new QuorumEffect.ReadLog(token,from,config.fetchMaxBytes()));
    }
    private void persistVote(long epoch,int voter,Runnable after) {
        var token=token();diskCallbacks.put(token,result->{
            if(result instanceof DiskResult.VoteSaved saved)persistedEpoch=Math.max(persistedEpoch,saved.epoch());after.run();
        });effects.add(new QuorumEffect.PersistVote(token,epoch,voter));
    }
    private void updatePhysical(DiskResult result) {
        if(result instanceof DiskResult.Appended appended){index=appended.index();progress(index.end(),appended.durableEnd(),status.commit(),status.applied());}
        else if(result instanceof DiskResult.Flushed flushed)progress(status.logEnd(),flushed.end(),status.commit(),status.applied());
        else if(result instanceof DiskResult.Truncated truncated){index=truncated.index();progress(index.end(),index.end(),status.commit(),status.applied());}
    }
    private void stepDown(long epoch,int leader){transition(QuorumStatus.Role.FOLLOWER,epoch,leader,false,"");votes.clear();voteRequests.clear();waitingFetches.clear();fetchFlight=null;echoChallenge=0;nextFetchAt=now;leaderCommit=status.commit();resetElectionDeadline();}
    private void resetElectionDeadline(){long min=config.electionMin().toNanos(),max=config.electionMax().toNanos();electionDeadline=now+min+random.nextLong(max-min);}
    private DiskToken token(){return new DiskToken(++operationId,status.epoch(),status.generation());}
    private Frame request(Request request){return new Frame(QuorumProtocol.operation(request),false,config.identity().clusterId(),status.nodeId(),++requestId,config.identity().voterHash(),request);}
    private void reply(PeerRequest request,Reply reply){effects.add(new QuorumEffect.Reply(request.route(),new Frame(request.frame().operation(),true,config.identity().clusterId(),status.nodeId(),request.frame().requestId(),config.identity().voterHash(),reply)));}
    private ReplyMeta meta(QuorumError error){return new ReplyMeta(error,"",status.epoch(),status.leaderId());}
    private Reply errorReply(Request request,QuorumError error){return request instanceof Vote?new VoteReply(meta(error),false):new Failure(meta(error));}
    private void fail(String failure){transition(QuorumStatus.Role.FAILED,status.epoch(),-1,false,failure);effects.add(new QuorumEffect.Fail(failure));}
    private void transition(QuorumStatus.Role role,long epoch,int leader,boolean ready,String failure){status=new QuorumStatus(status.nodeId(),role,epoch,leader,status.generation(),status.logEnd(),status.durableEnd(),status.commit(),status.applied(),status.snapshotEnd(),ready,status.durableMatches(),failure);}
    private void progress(long end,long durable,long commit,long applied){status=new QuorumStatus(status.nodeId(),status.role(),status.epoch(),status.leaderId(),status.generation(),end,durable,commit,applied,status.snapshotEnd(),status.ready(),status.durableMatches(),status.failure());}
    public QuorumStatus status(){return status;}
}

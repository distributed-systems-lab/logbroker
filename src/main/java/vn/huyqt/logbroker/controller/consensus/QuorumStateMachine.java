package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import java.util.random.RandomGenerator;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;

/** Pure transitions; I/O effects are interpreted outside this state owner. */
public final class QuorumStateMachine {
    private final ControllerConfig config;private QuorumStatus status;private EpochIndex index;private int votedFor;
    private final RandomGenerator random;
    private long persistedEpoch,now,electionDeadline,operationId,requestId;
    private final Map<DiskToken,Consumer<DiskResult>> diskCallbacks=new HashMap<>();
    private final Set<DiskToken> logWork=new HashSet<>();
    private final Map<Long,Integer> voteRequests=new HashMap<>();
    private final Set<Integer> votes=new HashSet<>();
    private List<QuorumEffect> effects;
    private boolean electionPending;
    public QuorumStateMachine(ControllerConfig config,QuorumStatus status,EpochIndex index,int votedFor,RandomGenerator random,long now) {
        this.config=config;this.status=status;this.index=index;this.votedFor=votedFor;this.random=random;
        this.now=now;persistedEpoch=status.epoch();resetElectionDeadline();
    }
    public List<QuorumEffect> on(QuorumEvent event) {
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
                if(status.role()!=QuorumStatus.Role.LEADER&&now>=electionDeadline&&!electionPending)prepareElection();
            } else if(event instanceof PeerRequest request&&validPeer(request.frame()))handleRequest(request);
            else if(event instanceof PeerResponse response&&validPeer(response.frame()))handleResponse(response.frame());
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
            persistVote(begin.epoch(),votedFor,()->{if(status.epoch()==begin.epoch()&&status.leaderId()==event.frame().senderId())reply(event,new EpochReply(meta(QuorumError.NONE)));});
        } else if(request instanceof EndQuorumEpoch end) {
            if(status.leaderId()==event.frame().senderId()){stepDown(end.epoch(),-1);electionDeadline=now;}
            if(persistedEpoch==status.epoch())reply(event,new EpochReply(meta(QuorumError.NONE)));
        }
    }
    private void handleResponse(Frame frame) {
        Reply reply=(Reply)frame.message();
        if(reply.meta().epoch()>status.epoch()) {
            long epoch=reply.meta().epoch();stepDown(epoch,-1);votedFor=-1;persistVote(epoch,-1,()->{});return;
        }
        if(reply instanceof VoteReply vote&&status.role()==QuorumStatus.Role.CANDIDATE&&vote.meta().epoch()==status.epoch()
            &&vote.meta().error()==QuorumError.NONE&&vote.granted()) {
            Integer expected=voteRequests.remove(frame.requestId());if(expected==null||expected!=frame.senderId())return;
            votes.add(frame.senderId());if(votes.size()>=2)becomeLeader();
        }
    }
    private void becomeLeader() {
        transition(QuorumStatus.Role.LEADER,status.epoch(),status.nodeId(),false,"");voteRequests.clear();
        for(var voter:config.identity().voters())if(voter.id()!=status.nodeId())effects.add(new QuorumEffect.Send(voter.id(),request(new BeginQuorumEpoch(status.epoch()))));
        var token=token();logWork.add(token);diskCallbacks.put(token,result->{
            if(status.role()==QuorumStatus.Role.LEADER&&status.epoch()==token.epoch()) {
                var flush=token();diskCallbacks.put(flush,ignored->{});effects.add(new QuorumEffect.Flush(flush));
            }
        });effects.add(new QuorumEffect.Append(token,status.epoch(),List.of(new QuorumEntry.LeaderChange(status.epoch(),status.nodeId()))));
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
    private void stepDown(long epoch,int leader){transition(QuorumStatus.Role.FOLLOWER,epoch,leader,false,"");votes.clear();voteRequests.clear();resetElectionDeadline();}
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

package vn.huyqt.logbroker.controller.consensus;

import java.util.*;
import java.util.random.RandomGenerator;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.log.EpochIndex;

/** Pure transitions; I/O effects are interpreted outside this state owner. */
public final class QuorumStateMachine {
    private final ControllerConfig config;private QuorumStatus status;private EpochIndex index;private int votedFor;
    private final RandomGenerator random;
    public QuorumStateMachine(ControllerConfig config,QuorumStatus status,EpochIndex index,int votedFor,RandomGenerator random,long now) {
        this.config=config;this.status=status;this.index=index;this.votedFor=votedFor;this.random=random;
    }
    public List<QuorumEffect> on(QuorumEvent event) {
        if(event instanceof QuorumEvent.Stop)status=withRole(QuorumStatus.Role.STOPPING,"");
        if(event instanceof QuorumEvent.DiskFailed failed)status=withRole(QuorumStatus.Role.FAILED,failed.failure());
        return List.of();
    }
    private QuorumStatus withRole(QuorumStatus.Role role,String failure){return new QuorumStatus(status.nodeId(),role,status.epoch(),-1,status.generation(),status.logEnd(),status.durableEnd(),status.commit(),status.applied(),status.snapshotEnd(),false,status.durableMatches(),failure);}
    public QuorumStatus status(){return status;}
}

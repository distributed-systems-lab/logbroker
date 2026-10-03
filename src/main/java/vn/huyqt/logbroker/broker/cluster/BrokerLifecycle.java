package vn.huyqt.logbroker.broker.cluster;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.client.ControllerClientException;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/** One process incarnation. Only fresh committed recovery decisions grant serving permission. */
public final class BrokerLifecycle {
    public enum State { STARTING, RECOVERING, FENCED, RUNNING, FAILED, STOPPING }
    private final BrokerIdentityStore.Identity identity;
    private final BrokerControlClient control;
    private final MetadataObserver observer;
    private final DeadlineScheduler clock;
    private final ServingGate gate;
    private final UUID incarnation=UUID.randomUUID();
    private final CompletableFuture<Void> started=new CompletableFuture<>();
    private CompletableFuture<Void> stopped;
    private State state=State.STARTING;
    private Session session;
    private long generation, expectedEpoch, registrationOffset, target, recoveryBase, grantOffset=-1, sequence, controllerEpoch;
    private UUID recoveryId;
    private boolean startedOnce,targetCaptured;
    private MetadataImage applied;
    private CompletableFuture<Reply> flight;
    private DeadlineScheduler.Ticket timer;
    public BrokerLifecycle(BrokerIdentityStore.Identity identity,BrokerControlClient control,MetadataObserver observer,
        DeadlineScheduler clock,ServingGate gate) {
        this.identity=Objects.requireNonNull(identity); this.control=Objects.requireNonNull(control);
        this.observer=Objects.requireNonNull(observer); this.clock=Objects.requireNonNull(clock); this.gate=Objects.requireNonNull(gate);
        if(!identity.clusterId().equals(control.settings().clusterId()) || identity.brokerId()!=control.settings().brokerId()
            || identity.brokerId()!=gate.session().brokerId() || !identity.storageId().equals(gate.session().storageId()))
            throw new IllegalArgumentException("Lifecycle identity mismatch");
        observer.attachLifecycle(image->marshal(()->metadataApplied(image)),error->marshal(()->failed(error)));
    }
    public synchronized CompletableFuture<Void> start() {
        if(startedOnce) throw new IllegalStateException("Lifecycle already started"); startedOnce=true; ++generation;
        discover(); return started;
    }
    private boolean active() { return startedOnce && state!=State.FAILED && state!=State.STOPPING; }
    private void marshal(Runnable action) { clock.schedule(clock.nanoTime(),action); }
    private void discover() {
        request(new ReadMetadata(2000),reply-> {
            if(!(reply instanceof BrokerControlProtocol.MetadataReply metadata) || metadata.consistency()!=Consistency.LINEARIZABLE
                || metadata.image().metadataVersion()!=2 || metadata.image().appliedOffset()>metadata.commitOffset()) {
                failed(new IOException("Invalid registration metadata")); return;
            }
            var existing=metadata.image().brokers().get(identity.brokerId());
            if(existing!=null && !existing.registration().session().storageId().equals(identity.storageId())) {
                failed(new IOException("Broker storage identity changed")); return;
            }
            expectedEpoch=existing==null ? -1 : existing.registration().session().brokerEpoch(); register();
        },this::discover);
    }
    private void register() {
        request(new Register(identity.brokerId(),identity.storageId(),incarnation,expectedEpoch,control.settings().advertised(),(short)2,(short)2,2000),reply-> {
            if(!(reply instanceof RegisterReply registered) || registered.session().brokerId()!=identity.brokerId()
                || !registered.session().storageId().equals(identity.storageId()) || !registered.session().incarnationId().equals(incarnation)
                || registered.session().brokerEpoch()!=registered.registrationOffset() || registered.registrationOffset()<=0
                || registered.requiredMetadataOffset()<registered.registrationOffset()) { failed(new IOException("Invalid committed registration")); return; }
            session=registered.session(); registrationOffset=registered.registrationOffset(); gate.bind(session);
            beginRecovery(registrationOffset,registered.requiredMetadataOffset());
            observer.start(session); heartbeat();
        },this::discover);
    }
    private void beginRecovery(long blocked,long required) {
        recoveryBase=Math.max(registrationOffset,blocked); target=Math.max(required,recoveryBase);
        targetCaptured=false; grantOffset=-1; recoveryId=UUID.randomUUID(); state=State.RECOVERING;
    }
    private void heartbeat() {
        if(!active() || session==null) return;
        UUID sentRecovery=state==State.RUNNING ? new UUID(0,0) : recoveryId;
        long end=applied==null ? observer.image().appliedOffset() : applied.appliedOffset();
        boolean complete=state!=State.RUNNING && targetCaptured && end>=target;
        request(new Heartbeat(session,++sequence,end,sentRecovery,complete,2000),reply-> {
            if(!(reply instanceof HeartbeatReply heartbeat) || heartbeat.meta().epoch()<controllerEpoch
                || heartbeat.meta().epoch()<control.controllerEpoch()) { schedule(this::heartbeat); return; }
            controllerEpoch=heartbeat.meta().epoch();
            if(!sentRecovery.equals(new UUID(0,0)) && !sentRecovery.equals(recoveryId)) { schedule(this::heartbeat); return; }
            if(heartbeat.brokerEpoch()!=session.brokerEpoch() && heartbeat.status()!=SessionStatus.STALE_SESSION
                && heartbeat.status()!=SessionStatus.REGISTRATION_REQUIRED) { failed(new IOException("Heartbeat epoch mismatch")); return; }
            if(heartbeat.status()!=SessionStatus.ACTIVE) {
                gate.reject(heartbeat.stateOffset());
                if(!gate.canServe()) {
                    if(state==State.RUNNING || heartbeat.stateOffset()>recoveryBase)
                        beginRecovery(heartbeat.stateOffset(),heartbeat.requiredMetadataOffset());
                    state=State.FENCED;
                }
            }
            if(state!=State.RUNNING && sentRecovery.equals(recoveryId) && heartbeat.recoveryId().equals(recoveryId)) {
                if(!targetCaptured) { target=Math.max(target,heartbeat.requiredMetadataOffset()); targetCaptured=true; }
                if(heartbeat.status()==SessionStatus.ACTIVE && complete && heartbeat.stateOffset()>recoveryBase)
                    grantOffset=heartbeat.stateOffset();
                if(applied!=null) evaluate(applied);
            }
            schedule(this::heartbeat);
        },this::heartbeat);
    }
    /** Called only for an immutable image published by the ordered observer worker. */
    public synchronized void metadataApplied(MetadataImage image) {
        if(!active() || session==null || applied!=null && image.appliedOffset()<applied.appliedOffset()) return;
        applied=image; evaluate(image);
    }
    private void evaluate(MetadataImage image) {
        var view=image.brokers().get(identity.brokerId());
        if(image.appliedOffset()<registrationOffset) return;
        if(view==null || !view.registration().session().equals(session)) {
            gate.reject(image.appliedOffset()); state=State.FENCED; return;
        }
        if(view.fenced()) {
            gate.apply(image.appliedOffset(),view.stateOffset(),true);
            if(state==State.RUNNING || view.stateOffset()>recoveryBase) beginRecovery(view.stateOffset(),image.appliedOffset());
            state=State.FENCED; return;
        }
        if(state==State.RUNNING || grantOffset>gate.blockedOffset() && grantOffset>recoveryBase
            && image.appliedOffset()>=grantOffset && view.stateOffset()>=grantOffset) {
            gate.apply(image.appliedOffset(),view.stateOffset(),false);
            if(gate.canServe()) { state=State.RUNNING; started.complete(null); }
        }
    }
    private void request(Request request,Consumer<Reply> accepted,Runnable retry) {
        if(!active()) return;
        if(flight!=null) throw new IllegalStateException("Concurrent lifecycle RPC");
        long token=generation; flight=control.request(request,clock.nanoTime()+2_000_000_000L); var current=flight;
        current.whenComplete((reply,error)->marshal(()-> { synchronized(BrokerLifecycle.this) {
            if(flight==current) flight=null;
            if(token!=generation || !active()) return;
            if(error==null) accepted.accept(reply);
            else if(error instanceof ControllerClientException failure && permanent(failure.error())) failed(error);
            else { if(session==null) state=State.FENCED; schedule(retry); }
        }}));
    }
    private boolean permanent(QuorumError error) {
        return error==QuorumError.STORAGE_ID_MISMATCH || error==QuorumError.CLUSTER_MISMATCH
            || error==QuorumError.INCONSISTENT_VOTER_SET || error==QuorumError.INCOMPATIBLE_METADATA_VERSION
            || error==QuorumError.INVALID_REQUEST;
    }
    private void schedule(Runnable action) {
        if(timer!=null) timer.cancel();
        timer=clock.schedule(clock.nanoTime()+control.settings().heartbeatInterval().toNanos(),()-> { synchronized(BrokerLifecycle.this) {
            timer=null; if(active()) action.run();
        }});
    }
    private synchronized void failed(Throwable error) {
        if(state==State.STOPPING || state==State.FAILED) return;
        state=State.FAILED; ++generation; gate.close(); if(timer!=null) timer.cancel();
        if(flight!=null) flight.cancel(false); flight=null; observer.stop(); started.completeExceptionally(error);
    }
    /** Closes admission immediately; storage/root ownership is released by composition after drain. */
    public synchronized CompletableFuture<Void> stop() {
        if(stopped!=null) return stopped;
        state=State.STOPPING; ++generation; gate.close(); if(timer!=null) timer.cancel();
        if(flight!=null) flight.cancel(false); flight=null;
        started.completeExceptionally(new CancellationException("Broker stopping")); stopped=observer.stop(); return stopped;
    }
    public synchronized State state() { return state; }
    public synchronized Session session() { return session; }
    public synchronized long recoveryTarget() { return target; }
}

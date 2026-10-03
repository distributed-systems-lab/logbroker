package vn.huyqt.logbroker.broker.cluster;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.DoubleSupplier;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.client.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.*;

/** Bounded, serialized broker RPCs on one reusable controller connection; never casts a vote. */
public final class BrokerControlClient implements AutoCloseable {
    private final BrokerClusterConfig config;
    private final ControllerClientTransport.Factory factory;
    private final DeadlineScheduler clock;
    private final DoubleSupplier jitter;
    private final ArrayDeque<Call> calls = new ArrayDeque<>();
    private List<ClusterIdentity.Voter> voters = List.of();
    private byte[] hash;
    private ControllerClientTransport wire;
    private long connection, requestId, epoch;
    private int targetIndex, leader = -1, pinned = -1;
    private boolean ready, discovered, connecting, waiting, closed, inboundPending;
    private short operation;
    private DeadlineScheduler.Ticket attemptTimer, backoff;

    public BrokerControlClient(BrokerClusterConfig config,ControllerClientTransport.Factory factory,DeadlineScheduler clock) {
        this(config,factory,clock,()->ThreadLocalRandom.current().nextDouble());
    }
    public BrokerControlClient(BrokerClusterConfig config,ControllerClientTransport.Factory factory,DeadlineScheduler clock,DoubleSupplier jitter) {
        this.config=Objects.requireNonNull(config); this.factory=Objects.requireNonNull(factory);
        this.clock=Objects.requireNonNull(clock); this.jitter=Objects.requireNonNull(jitter);
    }
    /** Keeps the caller's original absolute deadline through discovery, queueing and redirects. */
    public synchronized CompletableFuture<Reply> request(Request request,long deadline) {
        if (closed) return CompletableFuture.failedFuture(new IllegalStateException("Control client closed"));
        if (QuorumProtocol.operation(request)<106) return CompletableFuture.failedFuture(new IllegalArgumentException("Broker cannot call voter operations"));
        if (calls.size()>=16) return CompletableFuture.failedFuture(new RejectedExecutionException("Control RPC queue full"));
        var call=new Call(request,deadline); calls.add(call);
        call.timer=clock.schedule(deadline,()->expire(call));
        call.future.whenComplete((value,error)-> { if(call.future.isCancelled()) marshal(()->expire(call)); });
        drive(); return call.future;
    }
    private void marshal(Runnable action) { clock.schedule(clock.nanoTime(),action); }
    private synchronized void drive() {
        if (closed || calls.isEmpty() || waiting || connecting || backoff!=null) return;
        if (clock.nanoTime()>=calls.getFirst().deadline) { expire(calls.getFirst()); return; }
        if (wire==null) {
            long generation=++connection; wire=factory.create(); connecting=true; discovered=false; pinned=-1;
            InetSocketAddress address;
            var voter=voters.stream().filter(v->v.id()==leader).findFirst();
            if(voter.isPresent()) address=new InetSocketAddress(voter.get().host(),voter.get().port());
            else if(!voters.isEmpty()) { var v=voters.get(targetIndex++%voters.size()); address=new InetSocketAddress(v.host(),v.port()); }
            else { var endpoint=config.controllers().get(targetIndex++%config.controllers().size()); address=new InetSocketAddress(endpoint.host(),endpoint.port()); }
            armAttempt(generation);
            try { wire.connect(address,frame->enqueueReply(generation,frame),error->enqueueFailure(generation))
                .whenComplete((unused,error)->marshal(()->connected(generation,error))); }
            catch(RuntimeException e) { retry(generation); }
            return;
        }
        if(!ready) return;
        var call=calls.getFirst(); Request request=discovered ? remainingTimeout(call) : new DescribeQuorum();
        operation=QuorumProtocol.operation(request); long id=++requestId; waiting=true;
        armAttempt(connection);
        if(discovered || call.request instanceof DescribeQuorum) call.possiblySent=true;
        try { long generation=connection;
            wire.send(new Frame((short)2,SenderRole.BROKER,operation,false,config.clusterId(),config.brokerId(),id,new byte[32],request))
                .whenComplete((unused,error)-> { if(error!=null) marshal(()->retry(generation)); });
        } catch(RuntimeException e) { retry(connection); }
    }
    private synchronized void enqueueReply(long generation,Frame frame) {
        if(generation!=connection || !waiting || frame.requestId()!=requestId || inboundPending || closed) return;
        inboundPending=true;
        marshal(()-> { synchronized(BrokerControlClient.this) {
            if(generation!=connection) return; inboundPending=false; receive(generation,frame);
        }});
    }
    private synchronized void enqueueFailure(long generation) {
        if(generation!=connection || wire==null || inboundPending || closed) return;
        inboundPending=true;
        marshal(()-> { synchronized(BrokerControlClient.this) {
            if(generation!=connection) return; inboundPending=false; retry(generation);
        }});
    }
    private Request remainingTimeout(Call call) {
        int timeout=(int)Math.max(1,Math.min(30_000,(call.deadline-clock.nanoTime())/1_000_000));
        return switch(call.request) {
            case ReadMetadata read -> new ReadMetadata(Math.min(read.timeoutMs(),timeout));
            case Register r -> new Register(r.brokerId(),r.storageId(),r.incarnationId(),r.expectedBrokerEpoch(),r.endpoint(),r.minVersion(),r.maxVersion(),Math.min(r.timeoutMs(),timeout));
            case Heartbeat h -> new Heartbeat(h.session(),h.sequence(),h.appliedOffset(),h.recoveryId(),h.recoveryComplete(),Math.min(h.timeoutMs(),timeout));
            case BrokerControlProtocol.CreateTopic c -> new BrokerControlProtocol.CreateTopic(c.name(),c.partitions(),c.replicationFactor(),Math.min(c.timeoutMs(),timeout));
            default -> call.request;
        };
    }
    private synchronized void connected(long generation,Throwable error) {
        if(generation!=connection || wire==null || closed) return;
        connecting=false; if(error!=null) { retry(generation); return; } ready=true; drive();
    }
    private void armAttempt(long generation) {
        if(attemptTimer!=null) attemptTimer.cancel();
        attemptTimer=clock.schedule(Math.min(calls.getFirst().deadline,clock.nanoTime()+config.heartbeatRpcTimeout().toNanos()),()->retry(generation));
    }
    private synchronized void receive(long generation,Frame frame) {
        if(generation!=connection || wire==null || !waiting || calls.isEmpty() || frame.requestId()!=requestId) return;
        if(frame.version()!=2 || !frame.response() || frame.senderRole()!=SenderRole.VOTER
            || frame.operation()!=operation || !frame.clusterId().equals(config.clusterId()) || frame.senderId()<0
            || pinned>=0 && pinned!=frame.senderId() || !(frame.message() instanceof Reply)) {
            reject(QuorumError.CLUSTER_MISMATCH,"Invalid control reply identity"); return;
        }
        var reply=(Reply)frame.message();
        if(hash!=null && (!Arrays.equals(hash,frame.voterHash()) || voters.stream().noneMatch(v->v.id()==frame.senderId()))) {
            reject(QuorumError.INCONSISTENT_VOTER_SET,"Controller membership changed"); return;
        }
        if(!discovered) {
            if(!(reply instanceof DescribeReply description) || reply.meta().error()!=QuorumError.NONE) {
                if(reply.meta().error()==QuorumError.NOT_LEADER || reply.meta().error()==QuorumError.NODE_UNAVAILABLE) retry(generation);
                else reject(reply.meta().error()==QuorumError.NONE ? QuorumError.INVALID_REQUEST : reply.meta().error(),"Discovery rejected");
                return;
            }
            byte[] verified;
            try { verified=ClusterIdentity.voterHash(description.voters()); }
            catch(IllegalArgumentException e) { reject(QuorumError.INCONSISTENT_VOTER_SET,"Invalid membership"); return; }
            if(!Arrays.equals(verified,description.voterHash()) || !Arrays.equals(verified,frame.voterHash())
                || description.voters().stream().noneMatch(v->v.id()==frame.senderId())
                || description.status().nodeId()!=frame.senderId()) { reject(QuorumError.INCONSISTENT_VOTER_SET,"Invalid membership hash"); return; }
            hash=verified; voters=description.voters(); discovered=true;
        }
        pinned=frame.senderId(); epoch=Math.max(epoch,reply.meta().epoch());
        if(voters.stream().anyMatch(v->v.id()==reply.meta().leaderId())) leader=reply.meta().leaderId();
        if(operation==106 && !(calls.getFirst().request instanceof DescribeQuorum)) {
            waiting=false; if(attemptTimer!=null) attemptTimer.cancel(); drive(); return;
        }
        var error=reply.meta().error();
        if(error==QuorumError.NOT_LEADER || error==QuorumError.OVERLOADED || error==QuorumError.NODE_UNAVAILABLE || error==QuorumError.REQUEST_TIMED_OUT) {
            retry(generation); return;
        }
        if(error!=QuorumError.NONE) { reject(error,reply.meta().message()); return; }
        var call=calls.removeFirst(); waiting=false; call.timer.cancel(); if(attemptTimer!=null) attemptTimer.cancel();
        marshal(()->call.future.complete(reply)); drive();
    }
    private synchronized void retry(long generation) {
        if(generation!=connection || wire==null || calls.isEmpty() || closed) return;
        clearConnection();
        var call=calls.getFirst(); if(clock.nanoTime()>=call.deadline) { expire(call); return; }
        long delay=Math.min(1_000_000_000L,100_000_000L<<Math.min(call.retries++,4));
        delay=Math.min(1_000_000_000L,delay+(long)(Math.max(0,Math.min(1,jitter.getAsDouble()))*delay/4));
        backoff=clock.schedule(Math.min(call.deadline,clock.nanoTime()+delay),()-> {
            synchronized(BrokerControlClient.this) { backoff=null; drive(); }
        });
    }
    private synchronized void expire(Call call) {
        boolean first=calls.peekFirst()==call; if(!calls.remove(call)) return;
        call.timer.cancel(); if(first) clearConnection();
        marshal(()->call.future.completeExceptionally(exception(call,QuorumError.REQUEST_TIMED_OUT,"Control deadline expired"))); drive();
    }
    private void reject(QuorumError error,String message) {
        var call=calls.removeFirst(); call.timer.cancel(); clearConnection();
        marshal(()->call.future.completeExceptionally(exception(call,error,message))); drive();
    }
    private ControllerClientException exception(Call call,QuorumError error,String message) {
        return new ControllerClientException(error,call.possiblySent ? ControllerClientException.Outcome.UNKNOWN : ControllerClientException.Outcome.NOT_SENT,message);
    }
    private void clearConnection() {
        ++connection; if(wire!=null) wire.close(); wire=null; ready=connecting=waiting=discovered=inboundPending=false;
        if(attemptTimer!=null) attemptTimer.cancel(); if(backoff!=null) backoff.cancel(); backoff=null;
    }
    public synchronized byte[] voterHash() { if(hash==null) throw new IllegalStateException("Membership undiscovered"); return hash.clone(); }
    public synchronized long controllerEpoch() { return epoch; }
    @Override public synchronized void close() {
        if(closed) return; closed=true; clearConnection();
        for(var call:calls) { call.timer.cancel(); marshal(()->call.future.completeExceptionally(exception(call,QuorumError.NODE_UNAVAILABLE,"Control client closed"))); }
        calls.clear(); factory.close();
    }
    private static final class Call {
        final Request request; final long deadline; final CompletableFuture<Reply> future=new CompletableFuture<>();
        DeadlineScheduler.Ticket timer; int retries; boolean possiblySent;
        Call(Request request,long deadline) { this.request=request; this.deadline=deadline; }
    }
}

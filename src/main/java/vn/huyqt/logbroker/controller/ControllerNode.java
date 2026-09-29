package vn.huyqt.logbroker.controller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.runtime.*;
import vn.huyqt.logbroker.controller.snapshot.*;
import vn.huyqt.logbroker.controller.transport.*;

/** Recovery precedes publication of a listener. Root ownership outlives every disk task. */
public final class ControllerNode implements AutoCloseable {
    private final ControllerConfig config;private final QuorumStateStore state;private final GenerationStore generation;
    private final SnapshotStore snapshots;private final MetadataStateMachine metadata=new MetadataStateMachine();
    private final DeadlineScheduler clock;private final QuorumStateMachine core;
    private final ControllerLoop loop;private final OrderedDiskExecutor disk;private final EffectRunner runner;
    private final ControllerService service;private final NettyQuorumTransport transport;
    private final Map<DiskToken,QuorumTransport.Inbound> diskSources=new HashMap<>();
    private final Map<ReplyRoute,QuorumTransport.Inbound> peerSources=new HashMap<>();
    private final ConcurrentMap<ReplyRoute,QuorumTransport.Inbound> adminSources=new ConcurrentHashMap<>();
    private SnapshotCoordinator coordinator;private volatile DeadlineScheduler.Ticket ticker;
    private volatile boolean stopping,closed,started;private volatile String pendingFatal;
    private long appendedBytes;private CompletableFuture<Void> stopFence;
    private ControllerNode(ControllerConfig config,QuorumStateStore state,GenerationStore generation,SnapshotStore snapshots,MetadataImage image,long recoveredBytes)throws IOException {
        this.config=config;this.state=state;this.generation=generation;this.snapshots=snapshots;metadata.restore(image);
        clock=DeadlineScheduler.system();var index=generation.epochIndex();long snapshotEnd=generation.baseSnapshot()==null?0:generation.baseSnapshot().endOffset();
        var status=new QuorumStatus(config.identity().nodeId(),QuorumStatus.Role.UNATTACHED,state.epoch(),-1,generation.generation(),index.end(),generation.log().durableEnd(),generation.committedOffset(),image.appliedOffset(),snapshotEnd,false,Map.of(),"");
        core=new QuorumStateMachine(config,status,index,state.votedFor(),new Random(),clock.nanoTime(),clock::nanoTime,image);
        loop=new ControllerLoop(config.eventQueueCapacity(),512,config.diskQueueCapacity(),this::dispatch,true);
        disk=new OrderedDiskExecutor(config.diskQueueCapacity(),loop);
        service=new ControllerService(config.maxPendingRequests(),event->!stopping&&loop.submit(event,ControllerLoop.Priority.ADMIN),clock::nanoTime);
        transport=new NettyQuorumTransport(config,clock);runner=new EffectRunner(state,generation,metadata,disk,this::submitInternal,this::external);
        runner.snapshots(snapshots);runner.installFence(core::installAllowed);resetCoordinator();
        appendedBytes=recoveredBytes;
        if(!snapshots.retained().isEmpty())process(new SnapshotAvailable(snapshots.retained().getFirst()),null);
    }
    public static ControllerNode open(Path root,ControllerConfig config,DurableFiles files)throws IOException {
        QuorumStateStore state=null;GenerationStore generation=null;
        try {
            state=QuorumStateStore.open(root,config.identity(),files);generation=GenerationStore.open(state,files,config.logConfig());
            long logEpoch=generation.epochIndex().positionAt(generation.log().end()).lastEpoch();if(logEpoch>state.epoch())state.persistVote(logEpoch,-1);
            var image=generation.recoveredImage();var snapshots=new SnapshotStore(root,config.identity(),files,state,config.snapshotMaxBytes());
            var base=generation.baseSnapshot();if(base!=null&&(snapshots.retained().isEmpty()||snapshots.retained().getLast().endOffset()<base.endOffset()))snapshots.publishInstalled(base);
            long from=snapshots.retained().isEmpty()?generation.log().start():Math.max(generation.log().start(),snapshots.retained().getFirst().endOffset()),bytes=0;
            if(from>generation.log().end())throw new IOException("Published snapshot exceeds log coverage");
            while(from<generation.log().end()){var batches=generation.log().read(from,config.fetchMaxBytes());if(batches.isEmpty())throw new IOException("Snapshot byte-counter recovery gap");for(var batch:batches)bytes+=size(batch);from=batches.getLast().nextOffset();}
            return new ControllerNode(config,state,generation,snapshots,image,bytes);
        }catch(IOException|RuntimeException error){if(generation!=null)try{generation.close();}catch(IOException close){error.addSuppressed(close);}if(state!=null)try{state.close();}catch(IOException close){error.addSuppressed(close);}throw error;}
    }
    public synchronized InetSocketAddress start()throws IOException {
        if(started||stopping)throw new IllegalStateException("Controller already started or stopping");
        var voter=config.identity().voter(config.identity().nodeId());
        var bound=transport.start(new InetSocketAddress(voter.host(),voter.port()),this::receive,error->{/* RPC deadlines and elections handle network failure. */});
        started=true;scheduleTick();return bound;
    }
    public boolean isStarted(){return started;}
    public ControllerService service(){return service;}
    public CompletableFuture<QuorumStatus> status(){return CompletableFuture.completedFuture(core.status());}
    private void scheduleTick(){if(!stopping)ticker=clock.schedule(clock.nanoTime()+20_000_000L,()->{if(!stopping){loop.submit(new Tick(clock.nanoTime()),ControllerLoop.Priority.CONTROL);scheduleTick();}});}
    private void receive(QuorumTransport.Inbound inbound) {
        var frame=inbound.frame();var priority=frame.senderId()==-1?ControllerLoop.Priority.ADMIN:frame.operation()==105?ControllerLoop.Priority.SNAPSHOT:ControllerLoop.Priority.CONTROL;
        if(stopping||!loop.submit(new Network(inbound),priority)){inbound.reject();inbound.close();}
    }
    private void submitInternal(QuorumEvent event){if(!loop.submit(event,ControllerLoop.Priority.CONTROL))pendingFatal="Internal controller event admission exhausted";}
    private void dispatch(QuorumEvent event) {
        try {
            String fatal=pendingFatal;if(fatal!=null){pendingFatal=null;process(new Fatal(fatal),null);}
            if(event instanceof Network network)handleInbound(network.inbound());
            else if(event instanceof Invoke invoke)invoke.action().run();
            else {
                QuorumTransport.Inbound source=null;
                if(event instanceof DiskDone done)source=diskSources.remove(done.token());else if(event instanceof DiskFailed failed)source=diskSources.remove(failed.token());
                try {
                    if(event instanceof Tick)for(var route:List.copyOf(peerSources.keySet()))if(!peerSources.get(route).isActive()){peerSources.remove(route).close();}
                    if(event instanceof DiskDone done) {
                        if(done.result() instanceof DiskResult.Appended appended)appendedBytes+=size(appended.batch());
                        if(done.result() instanceof DiskResult.PrefixRetained retained&&done.token().generation().equals(core.status().generation()))process(new LogRetained(retained.index(),retained.base()),source);
                        coordinator.onCompletion(done);
                    }
                    process(event,source);
                    if(event instanceof DiskDone done&&done.result() instanceof DiskResult.Installed){appendedBytes=0;coordinator.close();resetCoordinator();}
                    if(event instanceof Applied applied&&(applied.generation()==null||applied.generation().equals(core.status().generation()))&&core.status().role()!=QuorumStatus.Role.FAILED&&core.status().role()!=QuorumStatus.Role.STOPPING)
                        coordinator.onApplied(metadata.image(),core.epochIndex().positionAt(core.status().applied()).lastEpoch(),appendedBytes);
                } finally {if(source!=null)source.close();}
            }
        }catch(RuntimeException error){process(new Fatal(error.toString()),null);}
    }
    private void handleInbound(QuorumTransport.Inbound inbound) {
        if(stopping||!inbound.isActive()){inbound.close();return;}
        var frame=inbound.frame();
        if(frame.senderId()==-1) {
            adminSources.put(inbound.route(),inbound);int timeout=frame.message() instanceof CreateTopic c?c.timeoutMs():frame.message() instanceof ReadMetadata r?r.timeoutMs():(int)config.adminTimeout().toMillis();
            service.request((Request)frame.message(),clock.nanoTime()+timeout*1_000_000L).whenComplete((reply,error)->{
                Reply response=reply;
                if(error!=null){Throwable cause=error instanceof CompletionException?error.getCause():error;response=new Failure(cause instanceof ControllerService.ServiceException failure?failure.meta():new ReplyMeta(QuorumError.NODE_UNAVAILABLE,"Request failed",core.status().epoch(),core.status().leaderId()));}
                var result=new Frame(frame.operation(),true,config.identity().clusterId(),config.identity().nodeId(),frame.requestId(),config.identity().voterHash(),response);
                transport.reply(inbound.route(),result).whenComplete((ignored,failed)->{var owned=adminSources.remove(inbound.route());if(owned!=null)owned.close();});
            });return;
        }
        try {
            if(core.status().role()==QuorumStatus.Role.FAILED){inbound.reject();return;}
            if(!frame.response())peerSources.put(inbound.route(),inbound.retain());
            process(frame.response()?new PeerResponse(frame):new PeerRequest(frame,inbound.route()),inbound);
        } finally {inbound.close();}
    }
    private void process(QuorumEvent event,QuorumTransport.Inbound source){runEffects(core.on(event),source);}
    private void runEffects(List<QuorumEffect> effects,QuorumTransport.Inbound source) {
        for(var effect:effects) {
            if(effect instanceof QuorumEffect.DiskEffect work&&source!=null)diskSources.put(work.token(),source.retain());
            runner.run(List.of(effect));
        }
    }
    private void external(QuorumEffect effect) {
        if(effect instanceof QuorumEffect.Send send){if(!stopping&&core.status().role()!=QuorumStatus.Role.FAILED)transport.send(send.peerId(),send.frame());}
        else if(effect instanceof QuorumEffect.Reply reply){transport.reply(reply.route(),reply.frame());var source=peerSources.remove(reply.route());if(source!=null)source.close();}
        else if(effect instanceof QuorumEffect.CompleteAdmin complete)service.complete(complete.invocationId(),complete.reply());
        else if(effect instanceof QuorumEffect.Fail fail) {
            if(core.status().role()!=QuorumStatus.Role.FAILED)process(new Fatal(fail.failure()),null);
            coordinator.close();for(var source:peerSources.values()){source.reject();source.close();}peerSources.clear();
        }
    }
    private void resetCoordinator(){coordinator=new SnapshotCoordinator(config.snapshotTriggerBytes(),core.status().generation(),()->core.status().epoch(),effect->runEffects(List.of(effect),null),id->process(new SnapshotAvailable(id),null),snapshots::retained);}
    static long size(QuorumBatch batch){return 30L+vn.huyqt.logbroker.storage.RecordPayloadCodec.encodedSize(batch.entries().stream().map(entry->new vn.huyqt.logbroker.storage.LogRecord(0,null,QuorumEntryCodec.encode(entry),List.of())).toList());}
    private static Duration remaining(long deadline)throws IOException {long left=deadline-System.nanoTime();if(left<=0)throw new IOException("Controller shutdown timed out");return Duration.ofNanos(left);}
    private void await(CompletableFuture<?> future,long deadline)throws IOException {try{future.get(remaining(deadline).toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException error){Thread.currentThread().interrupt();throw new IOException(error);}catch(ExecutionException|TimeoutException error){throw new IOException("Controller shutdown timed out or failed",error);}}
    @Override public synchronized void close()throws IOException {
        if(closed)return;long deadline=System.nanoTime()+config.shutdownTimeout().toNanos();stopping=true;transport.stopAccepting();if(ticker!=null)ticker.cancel();service.close();
        if(stopFence==null){stopFence=new CompletableFuture<>();submitInternal(new Stop());submitInternal(new Invoke(()->{coordinator.close();stopFence.complete(null);}));}
        await(stopFence,deadline);
        try {if(!disk.stop(remaining(deadline)))throw new IOException("Disk worker still active; controller storage lock retained");}
        catch(InterruptedException error){Thread.currentThread().interrupt();throw new IOException("Shutdown interrupted; storage lock retained",error);}
        var drained=new CompletableFuture<Void>();submitInternal(new Invoke(()->drained.complete(null)));await(drained,deadline);
        await(transport.closeAsync(),deadline);loop.close();
        for(var source:peerSources.values())source.close();peerSources.clear();for(var source:diskSources.values())source.close();diskSources.clear();
        for(var route:List.copyOf(adminSources.keySet())){var source=adminSources.remove(route);if(source!=null)source.close();}
        snapshots.close();generation.close();state.close();clock.close();closed=true;
        try {if(!service.awaitClosed(remaining(deadline)))throw new IOException("Response worker shutdown timed out");}
        catch(InterruptedException error){Thread.currentThread().interrupt();throw new IOException(error);}
    }
}

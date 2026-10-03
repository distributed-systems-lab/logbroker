package vn.huyqt.logbroker.broker.cluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.broker.metadata.*;
import vn.huyqt.logbroker.controller.client.*;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;
import vn.huyqt.logbroker.transport.netty.NettyServerTransport;

/** Resource composition for a cluster observer broker. No local topic metadata authority. */
public final class ClusterBrokerRuntime {
    private final BrokerIdentityStore identity;
    private final ObserverStore observerStore;
    private final DeadlineScheduler clock;
    private final ThreadPoolExecutor disk;
    private final PartitionExecutor lanes;
    private final BrokerControlClient control;
    private final MetadataObserver observer;
    private final BrokerLifecycle lifecycle;
    private final ServingGate gate;
    private final ClusterPartitionManager partitions;
    private final ClusterMetadataService metadata;
    private final FetchCoordinator fetch;
    private final RequestDispatcher dispatcher;
    private final NettyServerTransport transport;
    private final InetSocketAddress address;
    private CompletableFuture<Void> stopping;
    private ClusterBrokerRuntime(BrokerIdentityStore identity,ObserverStore store,DeadlineScheduler clock,ThreadPoolExecutor disk,
        PartitionExecutor lanes,BrokerControlClient control,MetadataObserver observer,BrokerLifecycle lifecycle,ServingGate gate,
        ClusterPartitionManager partitions,ClusterMetadataService metadata,FetchCoordinator fetch,RequestDispatcher dispatcher,
        NettyServerTransport transport,InetSocketAddress address) {
        this.identity=identity; this.observerStore=store; this.clock=clock; this.disk=disk; this.lanes=lanes;
        this.control=control; this.observer=observer; this.lifecycle=lifecycle; this.gate=gate; this.partitions=partitions;
        this.metadata=metadata; this.fetch=fetch; this.dispatcher=dispatcher; this.transport=transport; this.address=address;
    }
    public static ClusterBrokerRuntime start(BrokerConfig config,BrokerClusterConfig cluster,DurableFiles files,
        ControllerClientTransport.Factory suppliedFactory) throws IOException {
        // Fail before spawning workers or making an unformatted root look valid.
        var identity=BrokerIdentityStore.open(config.dataDirectory(),cluster.clusterId(),cluster.brokerId(),files);
        ObserverStore store=null; PartitionInventory inventory=null; DeadlineScheduler clock=null;
        ThreadPoolExecutor disk=null; PartitionExecutor lanes=null; BrokerControlClient control=null;
        NettyServerTransport transport=null; FetchCoordinator fetch=null;
        try {
            store=ObserverStore.open(config.dataDirectory().resolve("observer"),cluster.clusterId(),files,config.logConfig(),cluster.limits());
            inventory=PartitionInventory.open(config.dataDirectory(),identity.identity().storageId(),files);
            clock=DeadlineScheduler.system();
            disk=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(32),task-> {
                var thread=new Thread(task,"broker-cluster-disk"); thread.setDaemon(true); return thread;
            },new ThreadPoolExecutor.AbortPolicy());
            lanes=new PartitionExecutor(config.dataWorkers(),config.maxPartitions(),config.maxTasksPerPartition());
            control=new BrokerControlClient(cluster,suppliedFactory==null ? new NettyControllerClientTransport.Factory(cluster,clock) : suppliedFactory,clock);
            var gate=new ServingGate(new Session(cluster.brokerId(),identity.identity().storageId(),UUID.randomUUID(),0));
            var partitions=new ClusterPartitionManager(inventory,FilePartitionStore.clusterFactory(),disk,config.dataDirectory(),gate,files,config,lanes,clock);
            var owner=new AtomicReference<ClusterBrokerRuntime>();
            var observer=new MetadataObserver(control,store,clock,disk,image-> {
                var session=gate.session();
                if(session.brokerEpoch()>0) partitions.reconcile(image,session).whenComplete((unused,error)-> {
                    if(error!=null && !(error instanceof CancellationException)) {
                        var broker=owner.get(); if(broker!=null) broker.shutdown(config.shutdownTimeout());
                    }
                });
            },error-> { var broker=owner.get(); if(broker!=null) broker.shutdown(config.shutdownTimeout()); });
            var lifecycle=new BrokerLifecycle(identity.identity(),control,observer,clock,gate);
            var metadata=new ClusterMetadataService(control,observer::image,clock);
            java.util.function.Function<vn.huyqt.logbroker.protocol.Protocol.TopicPartition,PartitionRuntime> resolve=tp->partitions.runtime(tp).orElse(null);
            fetch=new FetchCoordinator(new FetchPlanner(resolve,config),resolve,clock,new ResourceBudget(config.maxRequestContexts()));
            var dispatcher=new RequestDispatcher(metadata,resolve,fetch,clock,cluster.clusterId(),gate);
            transport=new NettyServerTransport(config,clock);
            var address=transport.start(new InetSocketAddress(config.host(),config.port()),dispatcher);
            var runtime=new ClusterBrokerRuntime(identity,store,clock,disk,lanes,control,observer,lifecycle,gate,partitions,metadata,fetch,dispatcher,transport,address);
            owner.set(runtime);
            final DeadlineScheduler activeClock = clock;
            lifecycle.start().whenComplete((unused, error) -> {
                if (error != null && !(error instanceof CancellationException))
                    activeClock.schedule(activeClock.nanoTime(), () -> runtime.shutdown(config.shutdownTimeout()));
            });
            return runtime;
        } catch(Throwable error) {
            if(transport!=null) try { transport.closeAsync().get(); } catch(Exception e) { error.addSuppressed(e); }
            if(fetch!=null) fetch.close(); if(control!=null) control.close();
            if(lanes!=null) try { lanes.close(); } catch(Exception e) {
                error.addSuppressed(e); throw new IOException("Startup cleanup retains storage/root ownership",error);
            }
            if(disk!=null) { disk.shutdown(); try { if(!disk.awaitTermination(30,TimeUnit.SECONDS)) throw new IOException("Cluster disk did not stop"); }
                catch(Exception e) { error.addSuppressed(e); throw new IOException("Startup cleanup retains root ownership",error); } }
            if(store!=null) try { store.close(); } catch(Exception e) { error.addSuppressed(e); }
            if(inventory!=null) try { inventory.close(); } catch(Exception e) { error.addSuppressed(e); }
            if(clock!=null) clock.close(); try { identity.close(); } catch(Exception e) { error.addSuppressed(e); }
            if(error instanceof IOException io) throw io; throw new IOException("Cluster broker startup failed",error);
        }
    }
    public InetSocketAddress address() { return address; }
    public boolean canServe() { return gate.canServe(); }
    public BrokerMetadata metadata() { return metadata; }
    /** Any incomplete storage drain keeps the root lock and its files owned. */
    public synchronized CompletableFuture<Void> shutdown(Duration timeout) {
        if(stopping!=null) return stopping;
        gate.close(); transport.stopAccepting(); dispatcher.beginShutdown();
        var finished=new CompletableFuture<Void>(); stopping=finished.orTimeout(timeout.toMillis(),TimeUnit.MILLISECONDS);
        var stoppedObserver=lifecycle.stop();
        var thread=new Thread(()-> {
            try {
                stoppedObserver.get(); control.close();
                transport.closeAsync().get(); fetch.close();
                partitions.closeAsync().get(); lanes.close();
                disk.shutdown(); if(!disk.awaitTermination(timeout.toMillis(),TimeUnit.MILLISECONDS)) throw new IOException("Cluster disk did not drain");
                observerStore.close(); clock.close(); identity.close(); finished.complete(null);
            } catch(Throwable error) { finished.completeExceptionally(error); }
        },"broker-cluster-shutdown"); thread.setDaemon(true); thread.start(); return stopping;
    }
}

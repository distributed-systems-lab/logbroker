package vn.huyqt.logbroker.broker.cluster;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.storage.LogOpenOptions;

/** Owns assigned logs and inventory on one ordered worker; at most one active and one pending image. */
public final class ClusterPartitionManager {
    private final PartitionInventory inventory;
    private final PartitionStore.Factory factory;
    private final Executor disk;
    private final Path root;
    private final ServingGate gate;
    private final DurableFiles files;
    private final BrokerConfig config;
    private final PartitionExecutor lanes;
    private final DeadlineScheduler clock;
    private final ResourceBudget waiters;
    private final Map<TopicPartition,Owned> owned=new HashMap<>();
    private final Map<TopicPartition,String> failures=new HashMap<>();
    private Map<TopicPartition,PartitionRecord> desired=Map.of();
    private Session session;
    private long offset=-1;
    private Job pending,active;
    private boolean running,closed;
    private CompletableFuture<Void> stopped;
    private record Owned(Session session,PartitionStore store,PartitionRuntime runtime) {}
    private record Job(long offset,Session session,Map<TopicPartition,PartitionRecord> desired,CompletableFuture<Void> result) {}
    /** Takes ownership of inventory; the executor, lane pool, scheduler and root lock remain caller-owned. */
    public ClusterPartitionManager(PartitionInventory inventory,PartitionStore.Factory factory,Executor disk,Path root,
        ServingGate gate,DurableFiles files,BrokerConfig config,PartitionExecutor lanes,DeadlineScheduler clock) {
        this.inventory=Objects.requireNonNull(inventory); this.factory=Objects.requireNonNull(factory);
        this.disk=Objects.requireNonNull(disk); this.root=root.toAbsolutePath().normalize(); this.gate=Objects.requireNonNull(gate);
        this.files=Objects.requireNonNull(files); this.config=Objects.requireNonNull(config); this.lanes=Objects.requireNonNull(lanes);
        this.clock=Objects.requireNonNull(clock); waiters=new ResourceBudget(config.maxFlushedWaiters());
        gate.onChange(this::permissionChanged);
    }
    private void permissionChanged() {
        List<PartitionRuntime> runtimes;
        synchronized (this) { runtimes = owned.values().stream().map(Owned::runtime).toList(); }
        runtimes.forEach(PartitionRuntime::permissionChanged);
    }
    /** Updates desired ownership immediately; disk work never blocks the metadata caller. */
    public synchronized CompletableFuture<Void> reconcile(MetadataImage image,Session nextSession) {
        if(closed) return CompletableFuture.failedFuture(new IllegalStateException("Partition manager closed"));
        if(image.metadataVersion()!=2 || !nextSession.equals(gate.session())) return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid reconciliation session"));
        if(image.appliedOffset()<offset) return CompletableFuture.completedFuture(null);
        var assignments=new HashMap<TopicPartition,PartitionRecord>(); var view=image.brokers().get(nextSession.brokerId());
        if(view!=null && view.registration().session().equals(nextSession)) {
            for(var record:image.partitions().values()) if(record.leaderId()==nextSession.brokerId())
                assignments.put(new TopicPartition(record.topicId(),record.partitionId()),record);
        }
        var immutable=Map.copyOf(assignments);
        if(pending!=null && pending.offset()==image.appliedOffset() && pending.session().equals(nextSession) && pending.desired().equals(immutable)) return pending.result();
        if(active!=null && active.offset()==image.appliedOffset() && active.session().equals(nextSession) && active.desired().equals(immutable)) return active.result();
        desired=immutable; session=nextSession; offset=image.appliedOffset();
        if(pending!=null) pending.result().completeExceptionally(new CancellationException("Reconciliation superseded"));
        pending=new Job(offset,session,desired,new CompletableFuture<>()); var result=pending.result(); schedule(); return result;
    }
    private void schedule() {
        if(running) return; running=true;
        try { disk.execute(this::drain); }
        catch(RejectedExecutionException error) {
            running=false; if(pending!=null) { pending.result().completeExceptionally(error); pending=null; }
            if(stopped!=null) stopped.completeExceptionally(error);
        }
    }
    private void drain() {
        while(true) {
            Job job;
            synchronized(this) {
                if(closed) { job=null; }
                else if(pending==null) { running=false; active=null; return; }
                else { job=pending; pending=null; active=job; }
            }
            if(job==null) {
                Throwable error=null;
                try { closeAll(); inventory.close(); } catch(Exception e) { error=e; }
                synchronized(this) { running=false; active=null; if(error==null) stopped.complete(null); else stopped.completeExceptionally(error); }
                return;
            }
            try { work(job); job.result().complete(null); }
            catch(Exception error) { job.result().completeExceptionally(error); }
            synchronized(this) { active=null; }
        }
    }
    private void work(Job job) throws Exception {
        List<Map.Entry<TopicPartition,Owned>> existing;
        synchronized(this) { existing=List.copyOf(owned.entrySet()); }
        for(var entry:existing) {
            entry.getValue().runtime().permissionChanged();
            boolean keep;
            synchronized(this) { keep=!closed && session.equals(entry.getValue().session()) && desired.containsKey(entry.getKey()) && !failures.containsKey(entry.getKey()); }
            if(!keep) closeOwned(entry.getKey(),entry.getValue());
        }
        for(var entry:job.desired().entrySet()) {
            synchronized(this) { if(!current(job,entry.getKey(),entry.getValue()) || owned.containsKey(entry.getKey()) || failures.containsKey(entry.getKey())) continue; }
            provision(job,entry.getKey(),entry.getValue());
        }
    }
    private boolean current(Job job,TopicPartition partition,PartitionRecord record) {
        return !closed && job.session().equals(session) && session.equals(gate.session()) && record.equals(desired.get(partition));
    }
    private void provision(Job job,TopicPartition partition,PartitionRecord assignment) {
        PartitionStore store=null;
        try {
            Path parent=root.resolve("partitions"),topic=parent.resolve(partition.topicId().toString()),path=topic.resolve(Integer.toString(partition.partition()));
            if(!Files.isDirectory(parent,LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(topic) || Files.isSymbolicLink(path)) throw new IOException("Invalid partition directory");
            var state=inventory.state(partition);
            if(state==PartitionInventory.State.NEW && Files.exists(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Untracked partition directory retained without serving");
            if(state==PartitionInventory.State.COMPLETE && !hasData(path)) throw new IOException("Previously provisioned log missing: "+path);
            if(state==PartitionInventory.State.NEW) inventory.begin(partition);
            boolean create=state!=PartitionInventory.State.COMPLETE && !hasData(path);
            store=factory.open(path,config.logConfig(),new LogOpenOptions(0,0,create,files::syncDirectory));
            if(state!=PartitionInventory.State.COMPLETE) {
                store.flush();
                try(var paths=Files.list(path)) {
                    for(var file:paths.toList()) if(!file.getFileName().toString().equals(".lock")
                        && Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)) files.forceFile(file);
                }
                files.syncDirectory(path); files.syncDirectory(topic); files.syncDirectory(parent); inventory.complete(partition);
            }
            var runtime=new PartitionRuntime(partition,store,lanes,clock,config,waiters,error->markFailed(partition,error));
            synchronized(this) {
                if(current(job,partition,assignment)) { owned.put(partition,new Owned(job.session(),store,runtime)); store=null; }
            }
        } catch(Exception error) {
            synchronized(this) { if(current(job,partition,assignment)) failures.put(partition,error.toString()); }
        } finally {
            if(store!=null) try { store.close(); } catch(IOException error) { synchronized(this) { failures.putIfAbsent(partition,error.toString()); } }
        }
    }
    private boolean hasData(Path path) throws IOException {
        if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS)) return false;
        try(var files=Files.list(path)) { return files.anyMatch(file->Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS) && file.getFileName().toString().endsWith(".log")); }
    }
    private synchronized void markFailed(TopicPartition partition,Throwable error) {
        failures.putIfAbsent(partition,error.toString());
        if(!closed && pending==null) { pending=new Job(offset,session,desired,new CompletableFuture<>()); schedule(); }
    }
    public synchronized Optional<PartitionRuntime> runtime(TopicPartition partition) {
        var runtime=owned.get(partition);
        return closed || runtime==null || !runtime.session().equals(session) || !session.equals(gate.session())
            || !desired.containsKey(partition) || failures.containsKey(partition) ? Optional.empty() : Optional.of(runtime.runtime());
    }
    public synchronized Map<TopicPartition,String> failures() { return Map.copyOf(failures); }
    public synchronized Map<String,Integer> stateCounts() {
        int ready = 0;
        for (var partition : desired.keySet()) if (runtime(partition).isPresent()) ready++;
        return Map.of("desired", desired.size(), "ready", ready, "failed", failures.size(),
                "pending", Math.max(0, desired.size() - ready - failures.size()));
    }
    private void closeOwned(TopicPartition partition,Owned entry) throws Exception {
        // This ordered worker may wait; neither metadata nor Netty threads ever do.
        entry.runtime().closeAsync().get(); entry.store().flush(); entry.store().close();
        synchronized(this) { owned.remove(partition,entry); }
    }
    private void closeAll() throws Exception {
        List<Map.Entry<TopicPartition,Owned>> entries; synchronized(this) { entries=List.copyOf(owned.entrySet()); }
        Exception failure=null;
        for(var entry:entries) try { closeOwned(entry.getKey(),entry.getValue()); }
        catch(Exception error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
        if(failure!=null) throw failure;
    }
    /** Completes only after obsolete opens, partition lanes, stores and inventory have drained/closed. */
    public synchronized CompletableFuture<Void> closeAsync() {
        if(stopped!=null) return stopped; stopped=new CompletableFuture<>(); closed=true; desired=Map.of();
        if(pending!=null) { pending.result().completeExceptionally(new CancellationException("Partition manager closing")); pending=null; }
        schedule(); return stopped;
    }
}

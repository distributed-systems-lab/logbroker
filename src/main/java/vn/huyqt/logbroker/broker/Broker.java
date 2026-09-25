package vn.huyqt.logbroker.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import vn.huyqt.logbroker.broker.metadata.MetadataService;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.transport.netty.NettyServerTransport;

/** Owns recovery, listener admission, data workers, and data-root lifetime. */
public final class Broker implements AutoCloseable {
    private final BrokerConfig config;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final DeadlineScheduler clock;
    private final PartitionRegistry registry;
    private final MetadataService metadata;
    private final PartitionExecutor workers;
    private final Map<TopicPartition, PartitionRuntime> runtimes;
    private final FetchCoordinator fetch;
    private final RequestDispatcher dispatcher;
    private final NettyServerTransport transport;
    private final InetSocketAddress address;
    private CompletableFuture<Void> stopping;

    private Broker(BrokerConfig config, FileChannel lockChannel, FileLock lock,
                   DeadlineScheduler clock, PartitionRegistry registry,
                   MetadataService metadata, PartitionExecutor workers,
                   FetchCoordinator fetch, RequestDispatcher dispatcher,
                   NettyServerTransport transport, InetSocketAddress address,
                   Map<TopicPartition, PartitionRuntime> runtimes) {
        this.config = config; this.lockChannel = lockChannel; this.lock = lock;
        this.clock = clock; this.registry = registry; this.metadata = metadata;
        this.workers = workers; this.fetch = fetch; this.dispatcher = dispatcher;
        this.transport = transport; this.address = address; this.runtimes = runtimes;
    }

    public static Broker start(BrokerConfig config) throws IOException {
        Files.createDirectories(config.dataDirectory());
        FileChannel lockChannel = FileChannel.open(config.dataDirectory().resolve(".broker.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try { lock = lockChannel.tryLock(); }
        catch (java.nio.channels.OverlappingFileLockException busy) { lock = null; }
        if (lock == null) {
            lockChannel.close();
            throw new IOException("Broker data directory already in use");
        }
        DeadlineScheduler clock = null;
        PartitionRegistry registry = null;
        MetadataService metadata = null;
        PartitionExecutor workers = null;
        FetchCoordinator fetch = null;
        NettyServerTransport transport = null;
        try {
            clock = DeadlineScheduler.system();
            registry = new PartitionRegistry(config, FilePartitionStore::open);
            AtomicReference<Broker> owner = new AtomicReference<>();
            metadata = MetadataService.open(config.dataDirectory(), config, registry,
                    failure -> {
                        Broker broker = owner.get();
                        if (broker != null) broker.shutdown(config.shutdownTimeout());
                    });
            workers = new PartitionExecutor(config.dataWorkers(), config.maxPartitions(),
                    config.maxTasksPerPartition());
            final PartitionRegistry activeRegistry = registry;
            final PartitionExecutor activeWorkers = workers;
            final DeadlineScheduler activeClock = clock;
            final ResourceBudget flushedWaiters =
                    new ResourceBudget(config.maxFlushedWaiters());
            final Map<TopicPartition, PartitionRuntime> activeRuntimes = new ConcurrentHashMap<>();
            java.util.function.Function<TopicPartition, PartitionRuntime> resolve = tp -> {
                if (activeRegistry.state(tp) != vn.huyqt.logbroker.protocol.ErrorCode.NONE)
                    return null;
                return activeRuntimes.computeIfAbsent(tp, key -> {
                    try { return new PartitionRuntime(key, activeRegistry.require(key),
                            activeWorkers, activeClock, config,
                            flushedWaiters,
                            failure -> activeRegistry.markFailed(key, failure)); }
                    catch (IOException failure) { return null; }
                });
            };
            fetch = new FetchCoordinator(new FetchPlanner(resolve, config),
                    resolve, clock, new ResourceBudget(config.maxRequestContexts()));
            var dispatcher = new RequestDispatcher(metadata, resolve, fetch, clock);
            transport = new NettyServerTransport(config, clock);
            var address = transport.start(new InetSocketAddress(config.host(), config.port()),
                    dispatcher);
            var broker = new Broker(config, lockChannel, lock, clock, registry, metadata,
                    workers, fetch, dispatcher, transport, address, activeRuntimes);
            owner.set(broker);
            return broker;
        } catch (Throwable failure) {
            if (transport != null) try { transport.closeAsync().get(); }
                catch (Exception close) { failure.addSuppressed(close); }
            if (fetch != null) fetch.close();
            if (workers != null) try { workers.close(); }
                catch (Exception close) { failure.addSuppressed(close); }
            if (metadata != null) try { metadata.close(); }
                catch (Exception close) { failure.addSuppressed(close); }
            if (registry != null) try { registry.close(); }
                catch (Exception close) { failure.addSuppressed(close); }
            if (clock != null) clock.close();
            try { lock.release(); lockChannel.close(); }
            catch (IOException close) { failure.addSuppressed(close); }
            if (failure instanceof IOException io) throw io;
            throw new IOException("Broker startup failed", failure);
        }
    }

    public InetSocketAddress address() { return address; }

    public synchronized CompletableFuture<Void> shutdown(Duration deadline) {
        if (deadline == null || deadline.isZero() || deadline.isNegative())
            throw new IllegalArgumentException("Positive shutdown deadline required");
        if (stopping != null) return stopping;
        transport.stopAccepting();
        dispatcher.beginShutdown();
        var completion = new CompletableFuture<Void>();
        stopping = completion.orTimeout(deadline.toMillis(), TimeUnit.MILLISECONDS);
        var thread = new Thread(() -> {
            Throwable first = null;
            try {
                workers.drain().get(deadline.toMillis(), TimeUnit.MILLISECONDS);
                List<CompletableFuture<Void>> flushing = new ArrayList<>();
                for (var runtime : runtimes.values()) flushing.add(runtime.flushNow());
                CompletableFuture.allOf(flushing.toArray(CompletableFuture[]::new))
                        .get(deadline.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Throwable error) { first = error; }
            try { transport.closeAsync().get(); }
            catch (Throwable error) { if (first == null) first = error; else first.addSuppressed(error); }
            fetch.close();
            try { workers.close(); }
            catch (Throwable error) {
                if (first != null) error.addSuppressed(first);
                completion.completeExceptionally(error);
                return; // Keep stores and root lock while an I/O worker may still use them.
            }
            for (var runtime : runtimes.values()) runtime.close();
            try { metadata.close(); }
            catch (Throwable error) {
                if (first != null) error.addSuppressed(first);
                completion.completeExceptionally(error);
                return; // Metadata worker may still be writing its log.
            }
            try { registry.close(); }
            catch (Throwable error) { if (first == null) first = error; else first.addSuppressed(error); }
            clock.close();
            try { lock.release(); lockChannel.close(); }
            catch (Throwable error) { if (first == null) first = error; else first.addSuppressed(error); }
            if (first == null) completion.complete(null);
            else completion.completeExceptionally(first);
        }, "broker-shutdown");
        thread.setDaemon(true);
        thread.start();
        return completion;
    }

    @Override public void close() {
        try { shutdown(config.shutdownTimeout()).get(); }
        catch (Exception error) { throw new IllegalStateException("Broker shutdown failed", error); }
    }
}

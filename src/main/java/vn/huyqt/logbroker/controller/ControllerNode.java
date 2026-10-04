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

/**
 * Recovery precedes publication of a listener. Root ownership outlives every disk task.
 *
 * <p>Wires one controller: durable stores, the {@link QuorumStateMachine} core, the {@link
 * ControllerLoop} that owns it, the ordered disk executor, the {@link ControllerService} admission
 * front and the Netty transport. Consensus state changes only on the loop thread; inbound frames,
 * disk completions and timer ticks reach it as {@link QuorumEvent}s.
 *
 * <p>Lifecycle: {@link #open} recovers, {@link #start()} binds the listener once, and {@link
 * #close()} drains and releases the root. See {@code docs/controller-operation.md} for the operator
 * contract.
 */
public final class ControllerNode implements AutoCloseable {
    private final ControllerConfig config;
    private final QuorumStateStore state;
    private final GenerationStore generation;
    private final SnapshotStore snapshots;
    private final MetadataStateMachine metadata;
    private final DeadlineScheduler clock;
    private final QuorumStateMachine core;
    private final ControllerLoop loop;
    private final OrderedDiskExecutor disk;
    private final EffectRunner runner;
    private final ControllerService service;
    private final NettyQuorumTransport transport;
    private final Map<DiskToken, QuorumTransport.Inbound> diskSources = new HashMap<>();
    private final Map<DiskToken, vn.huyqt.logbroker.broker.ResourceBudget.Lease> readMemory =
            new HashMap<>();
    private final Map<ReplyRoute, QuorumTransport.Inbound> peerSources = new HashMap<>();
    private final ConcurrentMap<ReplyRoute, QuorumTransport.Inbound> adminSources =
            new ConcurrentHashMap<>();
    private SnapshotCoordinator coordinator;
    private volatile DeadlineScheduler.Ticket ticker;
    private volatile boolean stopping, closed, started;
    private volatile String pendingFatal;
    private long appendedBytes;
    private CompletableFuture<Void> stopFence;

    private ControllerNode(
            ControllerConfig config,
            QuorumStateStore state,
            GenerationStore generation,
            SnapshotStore snapshots,
            MetadataImage image,
            long recoveredBytes)
            throws IOException {
        this.config = config;
        this.state = state;
        this.generation = generation;
        this.snapshots = snapshots;
        metadata =
                new MetadataStateMachine(
                        config.metadataLimits(), config.identity().metadataVersion());
        metadata.restore(image);
        clock = DeadlineScheduler.system();
        var index = generation.epochIndex();
        long snapshotEnd =
                generation.baseSnapshot() == null ? 0 : generation.baseSnapshot().endOffset();
        var status =
                new QuorumStatus(
                        config.identity().nodeId(),
                        QuorumStatus.Role.UNATTACHED,
                        state.epoch(),
                        -1,
                        generation.generation(),
                        index.end(),
                        generation.log().durableEnd(),
                        generation.committedOffset(),
                        image.appliedOffset(),
                        snapshotEnd,
                        false,
                        Map.of(),
                        "");
        core =
                new QuorumStateMachine(
                        config,
                        status,
                        index,
                        state.votedFor(),
                        new Random(),
                        clock.nanoTime(),
                        clock::nanoTime,
                        image);
        loop =
                new ControllerLoop(
                        config.eventQueueCapacity(),
                        512,
                        config.diskQueueCapacity(),
                        this::dispatch,
                        true);
        disk = new OrderedDiskExecutor(config.diskQueueCapacity(), loop);
        service =
                new ControllerService(
                        config.maxPendingRequests(),
                        event -> !stopping && loop.submit(event, ControllerLoop.Priority.ADMIN),
                        clock::nanoTime);
        transport = new NettyQuorumTransport(config, clock);
        runner =
                new EffectRunner(
                        state, generation, metadata, disk, this::submitInternal, this::external);
        runner.snapshots(snapshots);
        runner.installFence(core::installAllowed);
        resetCoordinator();
        appendedBytes = recoveredBytes;
        if (!snapshots.retained().isEmpty())
            process(new SnapshotAvailable(snapshots.retained().getFirst()), null);
    }

    /**
     * Recovers a formatted root: opens the quorum state (taking the root lock), the active
     * generation and the snapshot store, and rebuilds the committed metadata image. Nothing is
     * bound or sent until {@link #start()}. On failure every store opened so far is closed.
     *
     * @throws IOException if the root is not formatted for {@code config}'s identity, is locked, or
     *     its durable state is inconsistent
     */
    public static ControllerNode open(Path root, ControllerConfig config, DurableFiles files)
            throws IOException {
        QuorumStateStore state = null;
        GenerationStore generation = null;
        try {
            state = QuorumStateStore.open(root, config.identity(), files);
            generation = GenerationStore.open(state, files, config.logConfig());
            // A crash can leave log entries from an epoch newer than the durable quorum state.
            // Persist
            // that epoch before communicating so the node never runs behind its own log.
            long logEpoch = generation.epochIndex().positionAt(generation.log().end()).lastEpoch();
            if (logEpoch > state.epoch()) state.persistVote(logEpoch, -1);
            var image =
                    generation.recoveredImage(
                            config.metadataLimits(), config.identity().metadataVersion());
            var snapshots =
                    new SnapshotStore(
                            root,
                            config.identity(),
                            files,
                            state,
                            config.snapshotMaxBytes(),
                            config.metadataLimits());
            var base = generation.baseSnapshot();
            // GenerationStore.install publishes the generation before
            // SnapshotStore.publishInstalled
            // records the snapshot set; a crash in between leaves the base outside the retained
            // set.
            if (base != null
                    && (snapshots.retained().isEmpty()
                            || snapshots.retained().getLast().endOffset() < base.endOffset()))
                snapshots.publishInstalled(base);
            // Recount storage bytes appended after the newest retained snapshot so the snapshot
            // trigger keeps its progress across restarts.
            long
                    from =
                            snapshots.retained().isEmpty()
                                    ? generation.log().start()
                                    : Math.max(
                                            generation.log().start(),
                                            snapshots.retained().getFirst().endOffset()),
                    bytes = 0;
            if (from > generation.log().end())
                throw new IOException("Published snapshot exceeds log coverage");
            while (from < generation.log().end()) {
                var batches = generation.log().read(from, config.fetchMaxBytes());
                if (batches.isEmpty()) throw new IOException("Snapshot byte-counter recovery gap");
                for (var batch : batches) bytes += size(batch);
                from = batches.getLast().nextOffset();
            }
            return new ControllerNode(config, state, generation, snapshots, image, bytes);
        } catch (IOException | RuntimeException error) {
            if (generation != null)
                try {
                    generation.close();
                } catch (IOException close) {
                    error.addSuppressed(close);
                }
            if (state != null)
                try {
                    state.close();
                } catch (IOException close) {
                    error.addSuppressed(close);
                }
            throw error;
        }
    }

    /**
     * Binds this node's configured voter endpoint and schedules the 20 ms {@link Tick} that drives
     * the core's timers. May be called once, and not after {@link #close()} began.
     *
     * @return the bound address
     * @throws IllegalStateException if already started or stopping
     */
    public synchronized InetSocketAddress start() throws IOException {
        var voter = config.identity().voter(config.identity().nodeId());
        return start(new InetSocketAddress(voter.host(), voter.port()));
    }

    /**
     * Package-private bind seam for the test fault proxy; membership endpoints remain unchanged.
     */
    synchronized InetSocketAddress start(InetSocketAddress bind) throws IOException {
        if (started || stopping)
            throw new IllegalStateException("Controller already started or stopping");
        var bound =
                transport.start(
                        bind,
                        this::receive,
                        error -> {
                            /* RPC deadlines and elections handle network failure. */
                        });
        started = true;
        scheduleTick();
        return bound;
    }

    public boolean isStarted() {
        return started;
    }

    public ControllerService service() {
        return service;
    }

    /**
     * Returns the core's most recently published local status, read without entering the loop. Peer
     * progress in it is what this node last observed, not a cluster-wide snapshot.
     */
    public CompletableFuture<QuorumStatus> status() {
        return CompletableFuture.completedFuture(core.status());
    }

    private void scheduleTick() {
        if (!stopping)
            ticker =
                    clock.schedule(
                            clock.nanoTime() + 20_000_000L,
                            () -> {
                                if (!stopping) {
                                    loop.submit(
                                            new Tick(clock.nanoTime()),
                                            ControllerLoop.Priority.CONTROL);
                                    scheduleTick();
                                }
                            });
    }

    // Admin (sender -1) and FetchSnapshot frames use the shared queue so they cannot exhaust the
    // capacity reserved for vote, epoch and fetch traffic.
    private void receive(QuorumTransport.Inbound inbound) {
        var frame = inbound.frame();
        var priority =
                frame.senderRole() != BrokerControlProtocol.SenderRole.VOTER
                                || frame.operation() >= 106
                        ? ControllerLoop.Priority.ADMIN
                        : frame.operation() == 105
                                ? ControllerLoop.Priority.SNAPSHOT
                                : ControllerLoop.Priority.CONTROL;
        if (stopping || !loop.submit(new Network(inbound), priority)) {
            inbound.reject();
            inbound.close();
        }
    }

    // A rejected internal event would be lost, so rejection fails the node at the next dispatch
    // instead of letting it continue without that event.
    private void submitInternal(QuorumEvent event) {
        if (!loop.submit(event, ControllerLoop.Priority.CONTROL))
            pendingFatal = "Internal controller event admission exhausted";
    }

    private void dispatch(QuorumEvent event) {
        try {
            String fatal = pendingFatal;
            if (fatal != null) {
                pendingFatal = null;
                process(new Fatal(fatal), null);
            }
            if (event instanceof Network network) handleInbound(network.inbound());
            else if (event instanceof Invoke invoke) invoke.action().run();
            else {
                QuorumTransport.Inbound source = null;
                vn.huyqt.logbroker.broker.ResourceBudget.Lease memory = null;
                if (event instanceof DiskDone done) source = diskSources.remove(done.token());
                else if (event instanceof DiskFailed failed)
                    source = diskSources.remove(failed.token());
                if (event instanceof DiskDone done) memory = readMemory.remove(done.token());
                else if (event instanceof DiskFailed failed)
                    memory = readMemory.remove(failed.token());
                try {
                    if (event instanceof Tick)
                        for (var route : List.copyOf(peerSources.keySet()))
                            if (!peerSources.get(route).isActive()) {
                                peerSources.remove(route).close();
                            }
                    if (event instanceof DiskDone done) {
                        if (done.result() instanceof DiskResult.Appended appended)
                            appendedBytes += size(appended.batch());
                        // Retention results for a generation already replaced by a snapshot install
                        // are stale.
                        if (done.result() instanceof DiskResult.PrefixRetained retained
                                && done.token().generation().equals(core.status().generation()))
                            process(new LogRetained(retained.index(), retained.base()), source);
                        coordinator.onCompletion(done);
                    }
                    process(event, source);
                    // An installed snapshot starts a new generation, so the trigger count and the
                    // coordinator, which is bound to one generation, start over.
                    if (event instanceof DiskDone done
                            && done.result() instanceof DiskResult.Installed) {
                        appendedBytes = 0;
                        coordinator.close();
                        resetCoordinator();
                    }
                    if (event instanceof Applied applied
                            && (applied.generation() == null
                                    || applied.generation().equals(core.status().generation()))
                            && core.status().role() != QuorumStatus.Role.FAILED
                            && core.status().role() != QuorumStatus.Role.STOPPING)
                        coordinator.onApplied(
                                metadata.image(),
                                core.epochIndex().positionAt(core.status().applied()).lastEpoch(),
                                appendedBytes);
                } finally {
                    if (source != null) source.close();
                    if (memory != null) memory.close();
                }
            }
        } catch (RuntimeException error) {
            process(new Fatal(error.toString()), null);
        }
    }

    private void handleInbound(QuorumTransport.Inbound inbound) {
        if (stopping || !inbound.isActive()) {
            inbound.close();
            return;
        }
        var frame = inbound.frame();
        // Network admin requests go through the service so they share its pending bound. The
        // inbound
        // reference is held until the reply write finishes.
        if (frame.senderRole() != BrokerControlProtocol.SenderRole.VOTER
                || frame.operation() >= 106) {
            adminSources.put(inbound.route(), inbound);
            int timeout =
                    frame.message() instanceof CreateTopic c
                            ? c.timeoutMs()
                            : frame.message() instanceof BrokerControlProtocol.Register r
                                    ? r.timeoutMs()
                                    : frame.message() instanceof BrokerControlProtocol.Heartbeat h
                                            ? h.timeoutMs()
                                            : frame.message()
                                                            instanceof
                                                            BrokerControlProtocol.CreateTopic c
                                                    ? c.timeoutMs()
                                                    : frame.message() instanceof ReadMetadata r
                                                            ? r.timeoutMs()
                                                            : (int)
                                                                    config.adminTimeout()
                                                                            .toMillis();
            service.request((Request) frame.message(), clock.nanoTime() + timeout * 1_000_000L)
                    .whenComplete(
                            (reply, error) -> {
                                Reply response = reply;
                                if (error != null) {
                                    Throwable cause =
                                            error instanceof CompletionException
                                                    ? error.getCause()
                                                    : error;
                                    response =
                                            new Failure(
                                                    cause
                                                                    instanceof
                                                                    ControllerService
                                                                                            .ServiceException
                                                                                    failure
                                                            ? failure.meta()
                                                            : new ReplyMeta(
                                                                    QuorumError.NODE_UNAVAILABLE,
                                                                    "Request failed",
                                                                    core.status().epoch(),
                                                                    core.status().leaderId()));
                                }
                                if (frame.version() == 2
                                        && response instanceof DescribeQuorumReply described) {
                                    response =
                                            new BrokerControlProtocol.DescribeReply(
                                                    described.meta(),
                                                    described.status(),
                                                    config.identity().voters(),
                                                    config.identity().voterHash());
                                } else if (frame.version() == 1
                                        && response
                                                instanceof BrokerControlProtocol.MetadataReply) {
                                    response =
                                            new Failure(
                                                    new ReplyMeta(
                                                            QuorumError.UNSUPPORTED_VERSION,
                                                            "Cluster metadata requires v2",
                                                            core.status().epoch(),
                                                            core.status().leaderId()));
                                } else if (frame.version() == 2
                                        && response instanceof MetadataReply) {
                                    // Full cluster reads are enabled by the v2 formatted controller
                                    // composition.
                                    response =
                                            new Failure(
                                                    new ReplyMeta(
                                                            QuorumError.UNSUPPORTED_VERSION,
                                                            "Controller metadata root is v1",
                                                            core.status().epoch(),
                                                            core.status().leaderId()));
                                }
                                var result =
                                        new Frame(
                                                frame.version(),
                                                BrokerControlProtocol.SenderRole.VOTER,
                                                frame.operation(),
                                                true,
                                                config.identity().clusterId(),
                                                config.identity().nodeId(),
                                                frame.requestId(),
                                                config.identity().voterHash(),
                                                response);
                                transport
                                        .reply(inbound.route(), result)
                                        .whenComplete(
                                                (ignored, failed) -> {
                                                    var owned =
                                                            adminSources.remove(inbound.route());
                                                    if (owned != null) owned.close();
                                                });
                            });
            return;
        }
        try {
            if (core.status().role() == QuorumStatus.Role.FAILED) {
                inbound.reject();
                return;
            }
            if (!frame.response()) peerSources.put(inbound.route(), inbound.retain());
            process(
                    frame.response()
                            ? new PeerResponse(frame)
                            : new PeerRequest(frame, inbound.route()),
                    inbound);
        } finally {
            inbound.close();
        }
    }

    private void process(QuorumEvent event, QuorumTransport.Inbound source) {
        runEffects(core.on(event), source);
    }

    private void runEffects(List<QuorumEffect> effects, QuorumTransport.Inbound source) {
        for (var effect : effects) {
            if (effect instanceof QuorumEffect.DiskEffect work && source != null)
                diskSources.put(work.token(), source.retain());
            // Reserve shared outbound memory before loading log bytes, held until the read
            // completes;
            // if none is available the read completes as overloaded rather than allocating.
            if (effect instanceof QuorumEffect.ReadLog read) {
                int budget = Math.min(read.budget(), config.logConfig().maxBatchBytes());
                var memory = transport.reserveReadMemory(budget);
                if (memory == null) {
                    submitInternal(new DiskDone(read.token(), new DiskResult.Overloaded()));
                    continue;
                }
                readMemory.put(read.token(), memory);
                runner.run(List.of(new QuorumEffect.ReadLog(read.token(), read.offset(), budget)));
                continue;
            }
            if (effect instanceof QuorumEffect.ReadObserver read) {
                int budget = Math.min(read.budget(), config.logConfig().maxBatchBytes());
                var memory = transport.reserveReadMemory(config.logConfig().maxBatchBytes());
                if (memory == null) {
                    submitInternal(new DiskDone(read.token(), new DiskResult.Overloaded()));
                    continue;
                }
                readMemory.put(read.token(), memory);
                runner.run(
                        List.of(
                                new QuorumEffect.ReadObserver(
                                        read.token(),
                                        read.session(),
                                        read.offset(),
                                        read.upperBound(),
                                        budget)));
                continue;
            }
            if (effect instanceof QuorumEffect.ReadObserverSnapshot read) {
                var memory = transport.reserveReadMemory(read.maxBytes());
                if (memory == null) {
                    submitInternal(new DiskDone(read.token(), new DiskResult.Overloaded()));
                    continue;
                }
                readMemory.put(read.token(), memory);
            }
            runner.run(List.of(effect));
        }
    }

    private void external(QuorumEffect effect) {
        if (effect instanceof QuorumEffect.Send send) {
            if (!stopping && core.status().role() != QuorumStatus.Role.FAILED)
                transport.send(send.peerId(), send.frame());
        } else if (effect instanceof QuorumEffect.Reply reply) {
            transport.reply(reply.route(), reply.frame());
            var source = peerSources.remove(reply.route());
            if (source != null) source.close();
        } else if (effect instanceof QuorumEffect.CompleteAdmin complete)
            service.complete(complete.invocationId(), complete.reply());
        else if (effect instanceof QuorumEffect.Fail fail) {
            if (core.status().role() != QuorumStatus.Role.FAILED)
                process(new Fatal(fail.failure()), null);
            coordinator.close();
            for (var source : peerSources.values()) {
                source.reject();
                source.close();
            }
            peerSources.clear();
        }
    }

    private void resetCoordinator() {
        coordinator =
                new SnapshotCoordinator(
                        config.snapshotTriggerBytes(),
                        core.status().generation(),
                        () -> core.status().epoch(),
                        effect -> runEffects(List.of(effect), null),
                        id -> process(new SnapshotAvailable(id), null),
                        snapshots::retained);
    }

    /**
     * Storage bytes of {@code batch}: the 30-byte storage batch header plus the record payload,
     * each entry being one keyless record. This is the unit counted against the snapshot trigger.
     */
    static long size(QuorumBatch batch) {
        return 30L
                + vn.huyqt.logbroker.storage.RecordPayloadCodec.encodedSize(
                        batch.entries().stream()
                                .map(
                                        entry ->
                                                new vn.huyqt.logbroker.storage.LogRecord(
                                                        0,
                                                        null,
                                                        QuorumEntryCodec.encode(entry),
                                                        List.of()))
                                .toList());
    }

    private static Duration remaining(long deadline) throws IOException {
        long left = deadline - System.nanoTime();
        if (left <= 0) throw new IOException("Controller shutdown timed out");
        return Duration.ofNanos(left);
    }

    private void await(CompletableFuture<?> future, long deadline) throws IOException {
        try {
            future.get(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException(error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IOException("Controller shutdown timed out or failed", error);
        }
    }

    /**
     * Stops admission, fails pending admin invocations, submits {@link Stop} to the core, drains
     * accepted disk work, then closes the transport and the stores and releases the root lock, all
     * within the configured shutdown timeout.
     *
     * <p>If the deadline passes before disk work has drained, this throws and keeps the root
     * locked, because files must not be closed while the disk worker may still modify them. Close
     * may then be retried; it reuses the stop already submitted. Once the stores are closed,
     * further calls do nothing, even if waiting for the response threads then timed out.
     *
     * @throws IOException if shutdown did not complete within the deadline or was interrupted
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        long deadline = System.nanoTime() + config.shutdownTimeout().toNanos();
        stopping = true;
        transport.stopAccepting();
        if (ticker != null) ticker.cancel();
        service.close();
        if (stopFence == null) {
            stopFence = new CompletableFuture<>();
            submitInternal(new Stop());
            submitInternal(
                    new Invoke(
                            () -> {
                                coordinator.close();
                                stopFence.complete(null);
                            }));
        }
        // Drain state transitions before stopping disk work; release the storage lock only after
        // disk
        // exit.
        await(stopFence, deadline);
        try {
            if (!disk.stop(remaining(deadline)))
                throw new IOException("Disk worker still active; controller storage lock retained");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("Shutdown interrupted; storage lock retained", error);
        }
        var drained = new CompletableFuture<Void>();
        submitInternal(new Invoke(() -> drained.complete(null)));
        await(drained, deadline);
        await(transport.closeAsync(), deadline);
        loop.close();
        for (var source : peerSources.values()) source.close();
        peerSources.clear();
        for (var source : diskSources.values()) source.close();
        diskSources.clear();
        for (var memory : readMemory.values()) memory.close();
        readMemory.clear();
        for (var route : List.copyOf(adminSources.keySet())) {
            var source = adminSources.remove(route);
            if (source != null) source.close();
        }
        snapshots.close();
        generation.close();
        state.close();
        clock.close();
        closed = true;
        try {
            if (!service.awaitClosed(remaining(deadline)))
                throw new IOException("Response worker shutdown timed out");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException(error);
        }
    }
}

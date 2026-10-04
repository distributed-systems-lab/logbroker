package vn.huyqt.logbroker.broker.cluster;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

/** Committed metadata pull loop. All storage work runs on the supplied ordered worker. */
public final class MetadataObserver {
    private final BrokerControlClient control;
    private final ObserverStore store;
    private final DeadlineScheduler clock;
    private final Executor disk;
    private final Consumer<MetadataImage> published;
    private final Consumer<Throwable> fatal;
    private Consumer<MetadataImage> lifecycleImage = ignored -> {};
    private Consumer<Throwable> lifecycleFailure = ignored -> {};
    private boolean attached;
    private volatile long generation;
    private volatile MetadataImage image;
    private Session session;
    private volatile boolean running;
    private boolean diskPending, cleaning;
    private CompletableFuture<Reply> flight;
    private CompletableFuture<Void> stopped = CompletableFuture.completedFuture(null);
    private DeadlineScheduler.Ticket retry;
    private SnapshotId snapshot;
    private long snapshotEpoch, position, total = -1;

    /**
     * Borrows the control client, store, scheduler and ordered disk executor. The composition owner
     * closes them only after {@link #stop()} completes and outstanding disk work drains.
     */
    public MetadataObserver(
            BrokerControlClient control,
            ObserverStore store,
            DeadlineScheduler clock,
            Executor disk,
            Consumer<MetadataImage> published,
            Consumer<Throwable> fatal) {
        this.control = Objects.requireNonNull(control);
        this.store = Objects.requireNonNull(store);
        this.clock = Objects.requireNonNull(clock);
        this.disk = Objects.requireNonNull(disk);
        this.published = Objects.requireNonNull(published);
        this.fatal = Objects.requireNonNull(fatal);
        image = store.image();
    }

    /** Starts a new pull generation only after the previous generation's disk work has drained. */
    public synchronized void start(Session session) {
        if (running || !stopped.isDone())
            throw new IllegalStateException("Observer must stop and drain before start");
        this.session = Objects.requireNonNull(session);
        running = true;
        ++generation;
        stopped = new CompletableFuture<>();
        discover(generation);
    }

    private void marshal(Runnable action) {
        clock.schedule(clock.nanoTime(), action);
    }

    private boolean current(long token) {
        return running && generation == token;
    }

    private void discover(long token) {
        request(
                token,
                new ReadMetadata(2000),
                (reply) -> {
                    if (!(reply instanceof BrokerControlProtocol.MetadataReply metadata)
                            || metadata.consistency() != Consistency.LINEARIZABLE
                            || metadata.image().metadataVersion() != 2
                            || metadata.image().appliedOffset() > metadata.commitOffset()) {
                        fail(new IOException("Incompatible or uncommitted controller metadata"));
                        return;
                    }
                    submit(
                            token,
                            () -> store.pinMembership(control.voterHash()),
                            () -> fetch(token));
                });
    }

    private void fetch(long token) {
        request(
                token,
                new ObserverFetch(
                        session,
                        control.controllerEpoch(),
                        image.appliedOffset(),
                        store.prefixEpoch(),
                        4 * 1024 * 1024,
                        100),
                reply -> {
                    if (!(reply instanceof ObserverFetchReply received)
                            || received.meta().epoch() != control.controllerEpoch()
                            || received.commitOffset() < image.appliedOffset()) {
                        restart(token);
                        return;
                    }
                    if (received.payload() instanceof FetchData data) {
                        long end = image.appliedOffset();
                        for (var batch : data.batches()) {
                            if (batch.baseOffset() != end
                                    || batch.nextOffset() > received.commitOffset()) {
                                fail(
                                        new IOException(
                                                "Observer received uncommitted or noncontiguous metadata"));
                                return;
                            }
                            end = batch.nextOffset();
                        }
                        if (data.batches().isEmpty()) {
                            later(token, () -> fetch(token));
                            return;
                        }
                        submit(
                                token,
                                () ->
                                        store.appendCommitted(
                                                data.batches(), received.commitOffset()),
                                () -> {
                                    publish();
                                    fetch(token);
                                });
                    } else if (received.payload() instanceof SnapshotRequired required) {
                        snapshot = required.id();
                        snapshotEpoch = received.meta().epoch();
                        position = 0;
                        total = -1;
                        if (snapshot.endOffset() > received.commitOffset()
                                || snapshot.endOffset() < image.appliedOffset()) {
                            fail(new IOException("Uncommitted observer snapshot"));
                            return;
                        }
                        submit(
                                token,
                                () -> store.snapshots().beginDownload(snapshot),
                                () -> chunk(token));
                    } else fail(new IOException("Unsupported observer history response"));
                });
    }

    private void chunk(long token) {
        var id = snapshot;
        long requested = position;
        request(
                token,
                new ObserverSnapshot(session, snapshotEpoch, id, requested, 256 * 1024),
                reply -> {
                    if (!(reply instanceof ObserverSnapshotReply received)
                            || received.meta().epoch() != snapshotEpoch
                            || !id.equals(received.id())
                            || received.position() != requested
                            || received.totalLength() < 114
                            || received.totalLength() > 64 * 1024 * 1024
                            || total != -1 && total != received.totalLength()
                            || received.chunkLength() == 0
                            || received.chunkLength() > 256 * 1024
                            || requested > received.totalLength() - received.chunkLength()) {
                        restart(token);
                        return;
                    }
                    total = received.totalLength();
                    byte[] bytes = received.chunk();
                    submit(
                            token,
                            () -> store.snapshots().writeChunk(id, requested, bytes),
                            () -> {
                                position = requested + bytes.length;
                                if (position < total) chunk(token);
                                else
                                    submit(
                                            token,
                                            () -> {
                                                var restored =
                                                        store.snapshots().finishDownload(id, total);
                                                store.install(
                                                        id,
                                                        restored,
                                                        () ->
                                                                current(token)
                                                                        && control.controllerEpoch()
                                                                                == snapshotEpoch);
                                                store.releaseObsoleteGenerations();
                                            },
                                            () -> {
                                                snapshot = null;
                                                publish();
                                                fetch(token);
                                            });
                            });
                });
    }

    synchronized void attachLifecycle(Consumer<MetadataImage> image, Consumer<Throwable> failure) {
        if (running || attached)
            throw new IllegalStateException("Lifecycle must attach before observer startup");
        lifecycleImage = Objects.requireNonNull(image);
        lifecycleFailure = Objects.requireNonNull(failure);
        attached = true;
    }

    private void publish() {
        image = store.image();
        published.accept(image);
        lifecycleImage.accept(image);
    }

    private void request(long token, Request request, Consumer<Reply> accepted) {
        if (!current(token)) return;
        if (flight != null || diskPending)
            throw new IllegalStateException("Observer pipeline overlap");
        flight = control.request(request, clock.nanoTime() + 2_000_000_000L);
        var expected = flight;
        expected.whenComplete(
                (reply, error) ->
                        marshal(
                                () -> {
                                    synchronized (MetadataObserver.this) {
                                        if (flight == expected) flight = null;
                                        if (!current(token)) return;
                                        if (error
                                                        instanceof
                                                        vn.huyqt.logbroker.controller.client
                                                                                .ControllerClientException
                                                                        refused
                                                && (refused.error() == QuorumError.CLUSTER_MISMATCH
                                                        || refused.error()
                                                                == QuorumError
                                                                        .INCONSISTENT_VOTER_SET
                                                        || refused.error()
                                                                == QuorumError
                                                                        .INCOMPATIBLE_METADATA_VERSION
                                                        || refused.error()
                                                                == QuorumError.INVALID_REQUEST))
                                            fail(error);
                                        else if (error != null) restart(token);
                                        else accepted.accept(reply);
                                    }
                                }));
    }

    private void later(long token, Runnable action) {
        retry =
                clock.schedule(
                        clock.nanoTime() + 100_000_000L,
                        () -> {
                            synchronized (MetadataObserver.this) {
                                retry = null;
                                if (current(token)) action.run();
                            }
                        });
    }

    private void restart(long token) {
        if (!current(token)) return;
        if (snapshot != null)
            submit(
                    token,
                    () -> store.snapshots().cancelDownload(),
                    () -> {
                        snapshot = null;
                        later(token, () -> fetch(token));
                    });
        else later(token, () -> fetch(token));
    }

    @FunctionalInterface
    private interface DiskAction {
        void run() throws Exception;
    }

    private void submit(long token, DiskAction action, Runnable completion) {
        if (diskPending) throw new IllegalStateException("Concurrent observer disk work");
        diskPending = true;
        try {
            disk.execute(
                    () -> {
                        Throwable failure = null;
                        try {
                            if (current(token)) action.run();
                        } catch (Exception e) {
                            failure = e;
                        }
                        var error = failure;
                        marshal(
                                () -> {
                                    synchronized (MetadataObserver.this) {
                                        diskPending = false;
                                        if (!current(token)) {
                                            drainStop();
                                            return;
                                        }
                                        if (error
                                                        instanceof
                                                        vn.huyqt.logbroker.controller.snapshot
                                                                .SnapshotStore
                                                                .InvalidDownloadContent
                                                || error instanceof ObserverStore.InstallCancelled)
                                            restart(token);
                                        else if (error != null) fail(error);
                                        else completion.run();
                                    }
                                });
                    });
        } catch (RejectedExecutionException e) {
            diskPending = false;
            fail(e);
        }
    }

    private void fail(Throwable error) {
        stop();
        fatal.accept(error);
        lifecycleFailure.accept(error);
    }

    /**
     * Invalidates callbacks immediately; completes only after disk work and download cleanup drain.
     */
    public synchronized CompletableFuture<Void> stop() {
        if (running) {
            running = false;
            ++generation;
            if (retry != null) retry.cancel();
            if (flight != null) flight.cancel(false);
            flight = null;
        }
        drainStop();
        return stopped;
    }

    private void drainStop() {
        if (running || stopped.isDone() || diskPending || cleaning) return;
        cleaning = true;
        try {
            disk.execute(
                    () -> {
                        Throwable failure = null;
                        try {
                            if (snapshot != null) store.snapshots().cancelDownload();
                        } catch (Exception e) {
                            failure = e;
                        }
                        var error = failure;
                        marshal(
                                () -> {
                                    synchronized (MetadataObserver.this) {
                                        cleaning = false;
                                        snapshot = null;
                                        if (error == null) stopped.complete(null);
                                        else stopped.completeExceptionally(error);
                                    }
                                });
                    });
        } catch (RejectedExecutionException error) {
            cleaning = false;
            stopped.completeExceptionally(error);
        }
    }

    public MetadataImage image() {
        return image;
    }
}

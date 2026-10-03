package vn.huyqt.logbroker.broker;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;
import vn.huyqt.logbroker.protocol.ClusterProtocol;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.AckMode;
import vn.huyqt.logbroker.protocol.Protocol.Batch;
import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.protocol.Protocol.FetchBatch;
import vn.huyqt.logbroker.protocol.Protocol.FetchEntry;
import vn.huyqt.logbroker.protocol.Protocol.FetchResult;
import vn.huyqt.logbroker.protocol.Protocol.ProduceResult;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.storage.AppendResult;
import vn.huyqt.logbroker.storage.RecordPayloadCodec;
import vn.huyqt.logbroker.protocol.WireBatchCodec;

/**
 * Serial partition operations and local durability acknowledgments.
 *
 * <p>Append, read and flush run as tasks on this partition's {@link PartitionExecutor} lane.
 * Apart from {@link #close()}, the dirty-batch queue, the {@code FLUSHED} waiters and the flush
 * timer are only touched from that lane. A {@code FLUSHED} produce parks a waiter instead of
 * holding the lane; the waiter completes once {@link PartitionStore#durableEndOffset()} covers
 * its batch. A flush is scheduled when unflushed storage bytes reach
 * {@link BrokerConfig#flushBytes()} or the oldest unflushed batch reaches
 * {@link BrokerConfig#flushInterval()}; see section 6 of
 * {@code docs/superpowers/specs/2026-09-25-broker-phase-2-design.md}.
 *
 * <p>The first storage failure is permanent for this runtime: parked waiters fail with
 * {@code STORAGE_ERROR}, later operations report {@code PARTITION_UNAVAILABLE}, and the failure
 * handler is invoked once so the owner can isolate the partition.
 */
public final class PartitionRuntime implements AutoCloseable {
    private final TopicPartition partition;
    private final PartitionStore store;
    private final PartitionExecutor executor;
    private final DeadlineScheduler clock;
    private final BrokerConfig config;
    private final ResourceBudget waiterBudget;
    private final Consumer<Throwable> failureHandler;
    private final ArrayDeque<Dirty> dirty = new ArrayDeque<>();
    private final List<Waiter> waiters = new ArrayList<>();
    private final Map<Long, Runnable> listeners = new HashMap<>();
    private long listenerId;
    private long dirtyBytes;
    private volatile long generation;
    private volatile boolean failed;
    private volatile boolean closed;
    private final java.util.concurrent.atomic.AtomicBoolean permissionDirty = new java.util.concurrent.atomic.AtomicBoolean();
    private CompletableFuture<Void> closeFuture;
    private DeadlineScheduler.Ticket flushTimer;

    /** Creates a runtime with a private waiter budget and a failure handler that does nothing. */
    public PartitionRuntime(TopicPartition partition, PartitionStore store,
            PartitionExecutor executor, DeadlineScheduler clock,
            BrokerConfig config) {
        this(partition, store, executor, clock, config,
                new ResourceBudget(config.maxFlushedWaiters()), error -> {
                });
    }

    /**
     * Creates a runtime over an open store.
     *
     * @param waiterBudget {@code FLUSHED} waiter capacity, usually shared by all partitions
     * @param failureHandler called once, on the lane, with the first storage failure
     */
    public PartitionRuntime(TopicPartition partition, PartitionStore store,
            PartitionExecutor executor, DeadlineScheduler clock,
            BrokerConfig config, ResourceBudget waiterBudget,
            Consumer<Throwable> failureHandler) {
        this.partition = Objects.requireNonNull(partition);
        this.store = Objects.requireNonNull(store);
        this.executor = Objects.requireNonNull(executor);
        this.clock = Objects.requireNonNull(clock);
        this.config = Objects.requireNonNull(config);
        this.waiterBudget = Objects.requireNonNull(waiterBudget);
        this.failureHandler = Objects.requireNonNull(failureHandler);
    }

    /**
     * Validates one batch and queues its append on the partition lane.
     *
     * <p>Record count, storage size and, for {@code FLUSHED}, waiter capacity are checked before
     * enqueue, so a rejected batch is never written. With {@code APPENDED} the result completes
     * once the local append returns; with {@code FLUSHED} once the durable end covers the batch.
     * Errors are reported in the {@link ProduceResult}:
     * <ul>
     * <li>{@code INVALID_REQUEST}, {@code BATCH_TOO_LARGE}, {@code OVERLOADED} (waiter budget or
     * lane queue full) and {@code PARTITION_UNAVAILABLE}: nothing was written.</li>
     * <li>{@code REQUEST_TIMED_OUT}: nothing was written if the deadline passed before the lane
     * started the append; for a parked {@code FLUSHED} waiter the batch was appended but its
     * durability is unconfirmed.</li>
     * <li>{@code STORAGE_ERROR} or {@code BROKER_SHUTTING_DOWN}: the outcome is unknown.</li>
     * </ul>
     *
     * @param deadlineNanos processing deadline on the {@link DeadlineScheduler#nanoTime()} scale
     */
    public CompletableFuture<ProduceResult> produce(Batch batch, AckMode mode, long deadlineNanos) {
        return produceInternal(batch, mode, deadlineNanos, new Operation(() -> true), false);
    }

    /** Checks permission on the lane before append; tracks mutation start independently of errors. */
    public CompletableFuture<ClusterProtocol.ProduceResult> produce(Batch batch, AckMode mode,
            long deadlineNanos, BooleanSupplier stillAllowed) {
        var operation = new Operation(Objects.requireNonNull(stillAllowed));
        return produceInternal(batch, mode, deadlineNanos, operation, true).thenApply(reply -> {
            boolean permitted = operation.allowed.getAsBoolean();
            if (reply.error().code() == ErrorCode.NONE && permitted)
                return new ClusterProtocol.ProduceResult(partition, reply.error(), ClusterProtocol.Outcome.SUCCESS,
                        reply.firstOffset(), reply.nextOffset());
            var error = reply.error().code() == ErrorCode.NONE
                    ? new Error(ErrorCode.FENCED_BROKER, "Permission changed after append") : reply.error();
            return new ClusterProtocol.ProduceResult(partition, error, operation.appendStarted
                    ? ClusterProtocol.Outcome.UNKNOWN : ClusterProtocol.Outcome.REJECTED, -1, -1);
        });
    }

    private CompletableFuture<ProduceResult> produceInternal(Batch batch, AckMode mode, long deadlineNanos,
            Operation operation, boolean timed) {
        Objects.requireNonNull(batch);
        Objects.requireNonNull(mode);
        var result = new CompletableFuture<ProduceResult>();
        if (closed || failed) {
            result.complete(error(ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable"));
            return result;
        }
        if (batch.records().isEmpty() || batch.records().size() > config.protocolLimits().maxRecordsPerBatch()) {
            result.complete(error(ErrorCode.INVALID_REQUEST, "Invalid record count"));
            return result;
        }
        // Storage batch size (30-byte header plus record payload, see docs/protocol-v1.md). A
        // batch that fits the wire limit can still exceed the storage limit.
        final int encodedBytes;
        try {
            encodedBytes = Math.addExact(30, RecordPayloadCodec.encodedSize(batch.records()));
        } catch (IllegalArgumentException | ArithmeticException badBatch) {
            result.complete(error(ErrorCode.BATCH_TOO_LARGE, "Invalid batch size"));
            return result;
        }
        if (encodedBytes > config.logConfig().maxBatchBytes()) {
            result.complete(error(ErrorCode.BATCH_TOO_LARGE, "Batch too large"));
            return result;
        }
        // Reserve waiter capacity before appending, so a batch is never written without room to
        // track its FLUSHED acknowledgment.
        ResourceBudget.Lease lease = null;
        if (mode == AckMode.FLUSHED) {
            lease = waiterBudget.reserve(1).orElse(null);
            if (lease == null) {
                result.complete(error(ErrorCode.OVERLOADED, "Too many flush waiters"));
                return result;
            }
        }
        ResourceBudget.Lease reserved = lease;
        DeadlineScheduler.Ticket deadline = timed ? clock.schedule(deadlineNanos, () -> {
            synchronized (operation) {
                result.complete(error(ErrorCode.REQUEST_TIMED_OUT, "Produce deadline passed"));
            }
        }) : null;
        if (deadline != null) result.whenComplete((ignored, failure) -> deadline.cancel());
        try {
            executor.submit(partition, () -> {
                appendOnLane(batch, mode, deadlineNanos, encodedBytes, reserved, result, operation);
                return null;
            });
        } catch (RejectedExecutionException rejected) {
            if (reserved != null)
                reserved.close();
            result.complete(error(ErrorCode.OVERLOADED, "Partition queue full"));
        }
        return result;
    }

    private void appendOnLane(Batch batch, AckMode mode, long deadlineNanos,
            int encodedBytes, ResourceBudget.Lease lease,
            CompletableFuture<ProduceResult> result, Operation operation) {
        if (closed || failed) {
            if (lease != null)
                lease.close();
            result.complete(error(ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable"));
            return;
        }
        // The deadline is only honored before mutation; a started append is never cancelled.
        if (clock.nanoTime() >= deadlineNanos) {
            if (lease != null)
                lease.close();
            result.complete(error(ErrorCode.REQUEST_TIMED_OUT, "Produce deadline passed"));
            return;
        }
        synchronized (operation) {
            if (result.isDone() || !operation.allowed.getAsBoolean() || clock.nanoTime() >= deadlineNanos) {
                if (lease != null) lease.close();
                result.complete(error(clock.nanoTime() >= deadlineNanos ? ErrorCode.REQUEST_TIMED_OUT : ErrorCode.FENCED_BROKER,
                        "Admission revoked before append"));
                return;
            }
            operation.appendStarted = true;
        }
        try {
            AppendResult appended = store.append(batch.records());
            long now = clock.nanoTime();
            dirty.addLast(new Dirty(appended.nextOffset(), encodedBytes, now));
            dirtyBytes = Math.addExact(dirtyBytes, encodedBytes);
            // A segment rollover during append may have forced earlier batches; settle them
            // now instead of waiting for an explicit flush.
            pruneDurable(store.durableEndOffset());
            generation++;
            notifyListeners();
            if (mode == AckMode.APPENDED) {
                result.complete(success(appended));
            } else if (store.durableEndOffset() >= appended.nextOffset()) {
                lease.close();
                result.complete(success(appended));
            } else {
                if (result.isDone()) lease.close();
                else {
                    Waiter waiter = new Waiter(appended, deadlineNanos, lease, result, operation);
                    waiters.add(waiter);
                    waiter.timer = clock.schedule(deadlineNanos, this::scheduleTick);
                }
            }
            scheduleFlush();
        } catch (IOException | RuntimeException failure) {
            if (lease != null)
                lease.close();
            fail(failure);
            result.complete(error(ErrorCode.STORAGE_ERROR, "Partition append failed"));
        }
    }

    private void pruneDurable(long durableEnd) {
        while (!dirty.isEmpty() && dirty.peekFirst().nextOffset <= durableEnd)
            dirtyBytes -= dirty.removeFirst().bytes;
        Iterator<Waiter> it = waiters.iterator();
        while (it.hasNext()) {
            Waiter waiter = it.next();
            if (!waiter.operation.allowed.getAsBoolean()) {
                waiter.finish(error(ErrorCode.FENCED_BROKER, "Permission revoked after append"));
                it.remove();
            } else if (waiter.appended.nextOffset() <= durableEnd) {
                waiter.finish(success(waiter.appended));
                it.remove();
            }
        }
    }

    private void scheduleFlush() {
        if (dirty.isEmpty() || failed || closed) {
            if (flushTimer != null) {
                flushTimer.cancel();
                flushTimer = null;
            }
            return;
        }
        if (dirtyBytes >= config.flushBytes()
                || clock.nanoTime() - dirty.peekFirst().atNanos >= config.flushInterval().toNanos()) {
            scheduleTick();
        } else if (flushTimer == null) {
            // The deadline is anchored at the oldest dirty batch and an armed timer is kept, so
            // later appends never postpone a due flush.
            long due = Math.addExact(dirty.peekFirst().atNanos,
                    config.flushInterval().toNanos());
            flushTimer = clock.schedule(due, this::scheduleTick);
        }
    }

    // Flush and waiter-expiry checks use the lane's control slot: they are not rejected when the
    // user queue is full, run ahead of queued user tasks, and coalesce while one is pending.
    private void scheduleTick() {
        if (!closed && !failed)
            executor.control(partition, this::tick);
    }

    private void tick() {
        if (flushTimer != null) {
            flushTimer.cancel();
            flushTimer = null;
        }
        if (closed || failed)
            return;
        settlePermissionChanges();
        if (!dirty.isEmpty() && (dirtyBytes >= config.flushBytes()
                || clock.nanoTime() - dirty.peekFirst().atNanos >= config.flushInterval().toNanos())) {
            try {
                pruneDurable(store.flush());
                generation++; notifyListeners();
            } catch (IOException | RuntimeException failure) {
                fail(failure);
                return;
            }
        }
        Iterator<Waiter> it = waiters.iterator();
        while (it.hasNext()) {
            Waiter waiter = it.next();
            if (clock.nanoTime() >= waiter.deadlineNanos) {
                // The batch is already in the log; only its durability is unconfirmed.
                waiter.finish(error(ErrorCode.REQUEST_TIMED_OUT,
                        "Produce may have been appended"));
                it.remove();
            }
        }
        scheduleFlush();
    }

    /**
     * Schedules a threshold check on the lane. It flushes only if the byte or age threshold is
     * already reached, and expires overdue {@code FLUSHED} waiters.
     */
    public void requestFlush() {
        scheduleTick();
    }

    /** Wakes FLUSHED and Fetch waiters on applied permission changes, never elapsed lease time. */
    public void permissionChanged() {
        if (!closed) {
            permissionDirty.set(true);
            scheduleTick();
        }
    }

    private void settlePermissionChanges() {
        if (permissionDirty.getAndSet(false)) {
            Iterator<Waiter> it = waiters.iterator();
            while (it.hasNext()) {
                Waiter waiter = it.next();
                if (!waiter.operation.allowed.getAsBoolean()) {
                    waiter.finish(error(ErrorCode.FENCED_BROKER, "Permission revoked after append")); it.remove();
                }
            }
            generation++; notifyListeners();
        }
    }

    /**
     * Forces all appended data regardless of thresholds and completes the waiters it covers.
     * Completes immediately if the runtime is closed or failed. A flush failure fails the
     * runtime and then the returned future.
     *
     * @throws RejectedExecutionException if the lane cannot accept another user task
     */
    public CompletableFuture<Void> flushNow() {
        if (closed || failed)
            return CompletableFuture.completedFuture(null);
        return executor.submit(partition, () -> {
            if (!closed && !failed) {
                try {
                    pruneDurable(store.flush());
                    generation++; notifyListeners();
                    scheduleFlush();
                } catch (IOException | RuntimeException failure) {
                    fail(failure);
                    throw failure;
                }
            }
            return null;
        });
    }

    /** Change counter, incremented after every successful append and on failure. */
    public long generation() {
        return generation;
    }

    /**
     * Reads whole batches from {@code entry.offset()} on the lane, up to {@code logEndOffset}
     * and therefore including unflushed data. Sizes are measured as Fetch wire bytes, not
     * storage bytes, against both {@code remainingWireBudget} and {@code entry.maxBytes()}.
     *
     * <p>Errors are returned in the {@link FetchResult}: {@code OFFSET_OUT_OF_RANGE} outside
     * {@code [logStartOffset, logEndOffset]}, {@code OVERLOADED} if the lane queue is full,
     * {@code PARTITION_UNAVAILABLE} after close or failure, and {@code STORAGE_ERROR} if the read
     * fails, which also fails the runtime.
     *
     * @param allowFirstOversize whether the first batch may exceed both budgets so the response
     *     makes progress; when used, no further batch is added
     */
    public CompletableFuture<FetchResult> read(FetchEntry entry, int remainingWireBudget,
            boolean allowFirstOversize) {
        if (closed || failed)
            return CompletableFuture.completedFuture(fetchError(
                    ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable"));
        try {
            return executor.submit(partition,
                    () -> readOnLane(entry, remainingWireBudget, allowFirstOversize));
        } catch (RejectedExecutionException rejected) {
            return CompletableFuture.completedFuture(fetchError(
                    ErrorCode.OVERLOADED, "Partition queue full"));
        }
    }

    private FetchResult readOnLane(FetchEntry entry, int totalBudget,
            boolean allowFirstOversize) {
        if (closed || failed)
            return fetchError(
                    ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable");
        return readOnLane(entry, totalBudget, allowFirstOversize, store.logStartOffset(), store.logEndOffset());
    }

    private FetchResult readOnLane(FetchEntry entry, int totalBudget, boolean allowFirstOversize, long start, long end) {
        if (entry.offset() < start || entry.offset() > end)
            return fetchError(ErrorCode.OFFSET_OUT_OF_RANGE, "Offset outside log");
        List<FetchBatch> result = new ArrayList<>();
        int remainingTotal = Math.max(0, totalBudget);
        int remainingPartition = entry.maxBytes();
        long cursor = entry.offset();
        try {
            while (cursor < end) {
                if (result.isEmpty() && !allowFirstOversize
                        && (remainingTotal == 0 || remainingPartition == 0))
                    break;
                var batches = store.read(cursor, 1);
                if (batches.isEmpty())
                    throw new IOException("Fetch made no progress");
                var stored = batches.getFirst();
                if (stored.nextOffset() > end) break;
                var batch = new Batch(stored.records());
                int wireBytes = WireBatchCodec.fetchSize(batch);
                boolean fits = wireBytes <= remainingTotal && wireBytes <= remainingPartition;
                if (!fits && (!result.isEmpty() || !allowFirstOversize))
                    break;
                result.add(new FetchBatch(stored.baseOffset(), batch));
                cursor = stored.nextOffset();
                remainingTotal = Math.max(0, remainingTotal - wireBytes);
                remainingPartition = Math.max(0, remainingPartition - wireBytes);
                if (!fits)
                    break;
            }
            return new FetchResult(partition, Error.none(), start, end, result);
        } catch (IOException | RuntimeException failure) {
            fail(failure);
            return fetchError(ErrorCode.STORAGE_ERROR, "Partition read failed");
        }
    }

    /** Captures RF1 HW from the durable end and reads only that prefix on the partition lane. */
    public CompletableFuture<ClusterProtocol.FetchResult> read(FetchEntry entry, int remainingWireBudget,
            boolean allowFirstOversize, BooleanSupplier stillAllowed) {
        Objects.requireNonNull(stillAllowed);
        try {
            return executor.submit(partition, () -> {
                if (!stillAllowed.getAsBoolean()) return clusterFetchError(ErrorCode.FENCED_BROKER, "Admission revoked");
                if (closed || failed) return clusterFetchError(ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable");
                long start = store.logStartOffset(), end = store.logEndOffset(), high = store.durableEndOffset();
                var read = readOnLane(entry, remainingWireBudget, allowFirstOversize, start, high);
                if (!stillAllowed.getAsBoolean()) return clusterFetchError(ErrorCode.FENCED_BROKER, "Admission revoked during read");
                if (read.error().code() != ErrorCode.NONE) return clusterFetchError(read.error().code(), read.error().message());
                return new ClusterProtocol.FetchResult(partition, read.error(), start, end, high, read.batches());
            });
        } catch (RejectedExecutionException error) {
            return CompletableFuture.completedFuture(clusterFetchError(ErrorCode.OVERLOADED, "Partition queue full"));
        }
    }

    private ClusterProtocol.FetchResult clusterFetchError(ErrorCode code, String message) {
        return new ClusterProtocol.FetchResult(partition, new Error(code, message), -1, -1, -1, List.of());
    }

    private FetchResult fetchError(ErrorCode code, String message) {
        return new FetchResult(partition, new Error(code, message), -1, -1, List.of());
    }

    /**
     * Registers {@code listener} to run after every append and on failure. Listeners run on the
     * partition lane and must not block.
     *
     * @return a ticket that unregisters the listener
     */
    public synchronized DeadlineScheduler.Ticket onChange(Runnable listener) {
        Objects.requireNonNull(listener);
        long id = ++listenerId;
        listeners.put(id, listener);
        return () -> {
            synchronized (PartitionRuntime.this) {
                return listeners.remove(id) != null;
            }
        };
    }

    private void notifyListeners() {
        List<Runnable> copy;
        synchronized (this) {
            copy = List.copyOf(listeners.values());
        }
        for (Runnable listener : copy)
            listener.run();
    }

    private void fail(Throwable failure) {
        if (failed)
            return;
        failed = true;
        generation++;
        if (flushTimer != null) {
            flushTimer.cancel();
            flushTimer = null;
        }
        for (Waiter waiter : waiters)
            waiter.finish(error(ErrorCode.STORAGE_ERROR, "Partition I/O failed"));
        waiters.clear();
        notifyListeners();
        failureHandler.accept(failure);
    }

    private ProduceResult success(AppendResult appended) {
        return new ProduceResult(partition, Error.none(), appended.firstOffset(), appended.nextOffset());
    }

    private ProduceResult error(ErrorCode code, String message) {
        return new ProduceResult(partition, new Error(code, message), -1, -1);
    }

    /**
     * Stops the runtime without flushing and fails parked {@code FLUSHED} waiters with
     * {@code BROKER_SHUTTING_DOWN}. The store is not closed; it belongs to the caller. Waiter
     * state is not synchronized with the lane; {@link Broker} calls this only after the
     * partition workers have stopped.
     */
    @Override
    public void close() {
        closed = true;
        if (flushTimer != null)
            flushTimer.cancel();
        for (Waiter waiter : waiters)
            waiter.finish(error(ErrorCode.BROKER_SHUTTING_DOWN, "Partition closing"));
        waiters.clear();
        synchronized (this) {
            listeners.clear();
        }
    }

    /** Stops admission immediately and finishes lane-owned waiters only after pending I/O drains. */
    public synchronized CompletableFuture<Void> closeAsync() {
        if(closeFuture!=null) return closeFuture;
        closed=true; generation++;
        if(flushTimer!=null) flushTimer.cancel();
        try { closeFuture=executor.afterPending(partition,this::close); }
        catch(RejectedExecutionException error) { closeFuture=CompletableFuture.failedFuture(error); }
        return closeFuture;
    }

    private record Dirty(long nextOffset, int bytes, long atNanos) {
    }

    private static final class Operation {
        final BooleanSupplier allowed;
        volatile boolean appendStarted;
        Operation(BooleanSupplier allowed) { this.allowed = allowed; }
    }

    private static final class Waiter {
        final AppendResult appended;
        final long deadlineNanos;
        final ResourceBudget.Lease lease;
        final CompletableFuture<ProduceResult> result;
        final Operation operation;
        DeadlineScheduler.Ticket timer;

        Waiter(AppendResult appended, long deadlineNanos, ResourceBudget.Lease lease,
                CompletableFuture<ProduceResult> result, Operation operation) {
            this.appended = appended;
            this.deadlineNanos = deadlineNanos;
            this.lease = lease;
            this.result = result;
            this.operation = operation;
        }

        void finish(ProduceResult outcome) {
            if (timer != null)
                timer.cancel();
            lease.close();
            result.complete(outcome);
        }
    }
}

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

/** Serial partition operations and local durability acknowledgments. */
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
    private DeadlineScheduler.Ticket flushTimer;

    public PartitionRuntime(TopicPartition partition, PartitionStore store,
                            PartitionExecutor executor, DeadlineScheduler clock,
                            BrokerConfig config) {
        this(partition, store, executor, clock, config,
                new ResourceBudget(config.maxFlushedWaiters()), error -> {});
    }

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

    public CompletableFuture<ProduceResult> produce(Batch batch, AckMode mode, long deadlineNanos) {
        Objects.requireNonNull(batch); Objects.requireNonNull(mode);
        var result = new CompletableFuture<ProduceResult>();
        if (closed || failed) {
            result.complete(error(ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable"));
            return result;
        }
        if (batch.records().isEmpty() || batch.records().size() > config.protocolLimits().maxRecordsPerBatch()) {
            result.complete(error(ErrorCode.INVALID_REQUEST, "Invalid record count"));
            return result;
        }
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
        ResourceBudget.Lease lease = null;
        if (mode == AckMode.FLUSHED) {
            lease = waiterBudget.reserve(1).orElse(null);
            if (lease == null) {
                result.complete(error(ErrorCode.OVERLOADED, "Too many flush waiters"));
                return result;
            }
        }
        ResourceBudget.Lease reserved = lease;
        try {
            executor.submit(partition, () -> {
                appendOnLane(batch, mode, deadlineNanos, encodedBytes, reserved, result);
                return null;
            });
        } catch (RejectedExecutionException rejected) {
            if (reserved != null) reserved.close();
            result.complete(error(ErrorCode.OVERLOADED, "Partition queue full"));
        }
        return result;
    }

    private void appendOnLane(Batch batch, AckMode mode, long deadlineNanos,
                              int encodedBytes, ResourceBudget.Lease lease,
                              CompletableFuture<ProduceResult> result) {
        if (closed || failed) {
            if (lease != null) lease.close();
            result.complete(error(ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable"));
            return;
        }
        if (clock.nanoTime() >= deadlineNanos) {
            if (lease != null) lease.close();
            result.complete(error(ErrorCode.REQUEST_TIMED_OUT, "Produce deadline passed"));
            return;
        }
        try {
            AppendResult appended = store.append(batch.records());
            long now = clock.nanoTime();
            dirty.addLast(new Dirty(appended.nextOffset(), encodedBytes, now));
            dirtyBytes = Math.addExact(dirtyBytes, encodedBytes);
            pruneDurable(store.durableEndOffset());
            generation++;
            notifyListeners();
            if (mode == AckMode.APPENDED) {
                result.complete(success(appended));
            } else if (store.durableEndOffset() >= appended.nextOffset()) {
                lease.close();
                result.complete(success(appended));
            } else {
                Waiter waiter = new Waiter(appended, deadlineNanos, lease, result);
                waiters.add(waiter);
                waiter.timer = clock.schedule(deadlineNanos, this::scheduleTick);
            }
            scheduleFlush();
        } catch (IOException | RuntimeException failure) {
            if (lease != null) lease.close();
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
            if (waiter.appended.nextOffset() <= durableEnd) {
                waiter.finish(success(waiter.appended));
                it.remove();
            }
        }
    }

    private void scheduleFlush() {
        if (dirty.isEmpty() || failed || closed) {
            if (flushTimer != null) { flushTimer.cancel(); flushTimer = null; }
            return;
        }
        if (dirtyBytes >= config.flushBytes()
                || clock.nanoTime() - dirty.peekFirst().atNanos >= config.flushInterval().toNanos()) {
            scheduleTick();
        } else if (flushTimer == null) {
            long due = Math.addExact(dirty.peekFirst().atNanos,
                    config.flushInterval().toNanos());
            flushTimer = clock.schedule(due, this::scheduleTick);
        }
    }

    private void scheduleTick() {
        if (!closed && !failed) executor.control(partition, this::tick);
    }

    private void tick() {
        if (flushTimer != null) { flushTimer.cancel(); flushTimer = null; }
        if (closed || failed) return;
        if (!dirty.isEmpty() && (dirtyBytes >= config.flushBytes()
                || clock.nanoTime() - dirty.peekFirst().atNanos >= config.flushInterval().toNanos())) {
            try {
                pruneDurable(store.flush());
            } catch (IOException | RuntimeException failure) {
                fail(failure);
                return;
            }
        }
        Iterator<Waiter> it = waiters.iterator();
        while (it.hasNext()) {
            Waiter waiter = it.next();
            if (clock.nanoTime() >= waiter.deadlineNanos) {
                waiter.finish(error(ErrorCode.REQUEST_TIMED_OUT,
                        "Produce may have been appended"));
                it.remove();
            }
        }
        scheduleFlush();
    }

    public void requestFlush() { scheduleTick(); }
    public long generation() { return generation; }

    public CompletableFuture<FetchResult> read(FetchEntry entry, int remainingWireBudget,
                                               boolean allowFirstOversize) {
        if (closed || failed) return CompletableFuture.completedFuture(fetchError(
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
        if (closed || failed) return fetchError(
                ErrorCode.PARTITION_UNAVAILABLE, "Partition unavailable");
        long start = store.logStartOffset(), end = store.logEndOffset();
        if (entry.offset() < start || entry.offset() > end)
            return fetchError(ErrorCode.OFFSET_OUT_OF_RANGE, "Offset outside log");
        List<FetchBatch> result = new ArrayList<>();
        int remainingTotal = Math.max(0, totalBudget);
        int remainingPartition = entry.maxBytes();
        long cursor = entry.offset();
        try {
            while (cursor < end) {
                if (result.isEmpty() && !allowFirstOversize
                        && (remainingTotal == 0 || remainingPartition == 0)) break;
                var batches = store.read(cursor, 1);
                if (batches.isEmpty()) throw new IOException("Fetch made no progress");
                var stored = batches.getFirst();
                var batch = new Batch(stored.records());
                int wireBytes = WireBatchCodec.fetchSize(batch);
                boolean fits = wireBytes <= remainingTotal && wireBytes <= remainingPartition;
                if (!fits && (!result.isEmpty() || !allowFirstOversize)) break;
                result.add(new FetchBatch(stored.baseOffset(), batch));
                cursor = stored.nextOffset();
                remainingTotal = Math.max(0, remainingTotal - wireBytes);
                remainingPartition = Math.max(0, remainingPartition - wireBytes);
                if (!fits) break;
            }
            return new FetchResult(partition, Error.none(), start, end, result);
        } catch (IOException | RuntimeException failure) {
            fail(failure);
            return fetchError(ErrorCode.STORAGE_ERROR, "Partition read failed");
        }
    }

    private FetchResult fetchError(ErrorCode code, String message) {
        return new FetchResult(partition, new Error(code, message), -1, -1, List.of());
    }

    public synchronized DeadlineScheduler.Ticket onChange(Runnable listener) {
        Objects.requireNonNull(listener);
        long id = ++listenerId;
        listeners.put(id, listener);
        return () -> { synchronized (PartitionRuntime.this) {
            return listeners.remove(id) != null;
        }};
    }

    private void notifyListeners() {
        List<Runnable> copy;
        synchronized (this) { copy = List.copyOf(listeners.values()); }
        for (Runnable listener : copy) listener.run();
    }

    private void fail(Throwable failure) {
        if (failed) return;
        failed = true;
        generation++;
        if (flushTimer != null) { flushTimer.cancel(); flushTimer = null; }
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

    @Override public void close() {
        closed = true;
        if (flushTimer != null) flushTimer.cancel();
        for (Waiter waiter : waiters)
            waiter.finish(error(ErrorCode.BROKER_SHUTTING_DOWN, "Partition closing"));
        waiters.clear();
        synchronized (this) { listeners.clear(); }
    }

    private record Dirty(long nextOffset, int bytes, long atNanos) {}

    private static final class Waiter {
        final AppendResult appended;
        final long deadlineNanos;
        final ResourceBudget.Lease lease;
        final CompletableFuture<ProduceResult> result;
        DeadlineScheduler.Ticket timer;

        Waiter(AppendResult appended, long deadlineNanos, ResourceBudget.Lease lease,
               CompletableFuture<ProduceResult> result) {
            this.appended = appended;
            this.deadlineNanos = deadlineNanos;
            this.lease = lease;
            this.result = result;
        }

        void finish(ProduceResult outcome) {
            if (timer != null) timer.cancel();
            lease.close();
            result.complete(outcome);
        }
    }
}

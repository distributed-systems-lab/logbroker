package vn.huyqt.logbroker.broker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

/** Runs at most one task per partition while sharing a bounded worker pool. */
public final class PartitionExecutor implements AutoCloseable {
    private final int partitionLimit;
    private final int queuedTaskLimit;
    private final ThreadPoolExecutor workers;
    private final Map<TopicPartition, Lane> lanes = new HashMap<>();
    private final List<CompletableFuture<Void>> drainWaiters = new ArrayList<>();
    private long outstanding;
    private boolean closed;

    public PartitionExecutor(int workerCount, int partitionLimit, int queuedTaskLimit) {
        if (workerCount <= 0 || partitionLimit <= 0 || queuedTaskLimit <= 0)
            throw new IllegalArgumentException("Invalid executor limits");
        this.partitionLimit = partitionLimit;
        this.queuedTaskLimit = queuedTaskLimit;
        workers = new ThreadPoolExecutor(workerCount, workerCount, 0,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(partitionLimit), task -> {
                    var thread = new Thread(task, "broker-partition");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    public synchronized <V> CompletableFuture<V> submit(TopicPartition key, Callable<V> action) {
        Objects.requireNonNull(key); Objects.requireNonNull(action);
        Lane lane = lane(key);
        if (lane.userTasks.size() >= queuedTaskLimit)
            throw new RejectedExecutionException("Partition queue full");
        CompletableFuture<V> result = new CompletableFuture<>();
        lane.userTasks.addLast(() -> {
            try { result.complete(action.call()); }
            catch (Throwable error) { result.completeExceptionally(error); }
        });
        outstanding++;
        schedule(key, lane);
        return result;
    }

    public synchronized void control(TopicPartition key, Runnable action) {
        Objects.requireNonNull(key); Objects.requireNonNull(action);
        Lane lane = lane(key);
        if (lane.control == null) {
            lane.control = action;
            outstanding++;
            schedule(key, lane);
        }
    }

    public synchronized CompletableFuture<Void> drain() {
        if (outstanding == 0) return CompletableFuture.completedFuture(null);
        var result = new CompletableFuture<Void>();
        drainWaiters.add(result);
        return result;
    }

    private Lane lane(TopicPartition key) {
        if (closed) throw new RejectedExecutionException("Partition executor closed");
        Lane existing = lanes.get(key);
        if (existing != null) return existing;
        if (lanes.size() >= partitionLimit)
            throw new RejectedExecutionException("Partition capacity reached");
        Lane created = new Lane();
        lanes.put(key, created);
        return created;
    }

    private void schedule(TopicPartition key, Lane lane) {
        if (lane.scheduled) return;
        lane.scheduled = true;
        workers.execute(() -> runOne(key, lane));
    }

    private void runOne(TopicPartition key, Lane lane) {
        Runnable task;
        synchronized (this) {
            if (lane.control != null) {
                task = lane.control;
                lane.control = null;
            } else {
                task = lane.userTasks.removeFirst();
            }
        }
        try { task.run(); }
        finally {
            synchronized (this) {
                outstanding--;
                if (lane.control != null || !lane.userTasks.isEmpty()) {
                    workers.execute(() -> runOne(key, lane));
                } else {
                    lane.scheduled = false;
                }
                if (outstanding == 0) {
                    drainWaiters.forEach(waiter -> waiter.complete(null));
                    drainWaiters.clear();
                }
            }
        }
    }

    @Override public void close() {
        synchronized (this) { closed = true; }
        try {
            drain().get(30, TimeUnit.SECONDS);
            workers.shutdown();
            if (!workers.awaitTermination(30, TimeUnit.SECONDS))
                throw new IllegalStateException("Partition workers did not stop");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted closing partition workers", error);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException error) {
            throw new IllegalStateException("Partition workers did not drain", error);
        }
    }

    private static final class Lane {
        final ArrayDeque<Runnable> userTasks = new ArrayDeque<>();
        Runnable control;
        boolean scheduled;
    }
}

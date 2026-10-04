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

/**
 * Runs at most one task per partition while sharing a bounded worker pool.
 *
 * <p>Each partition has a lane: a bounded FIFO of user tasks plus at most one pending control task.
 * Tasks of one lane never run concurrently and user tasks run in submission order, so state touched
 * only by one lane's tasks needs no further locking. A worker runs one task and then yields the
 * pool, so a busy partition cannot monopolize a thread. Lanes are created on first use and are not
 * removed.
 */
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
        // A lane has at most one runnable in the pool at a time and there are at most
        // partitionLimit lanes, so the pool queue sized to partitionLimit cannot overflow.
        workers =
                new ThreadPoolExecutor(
                        workerCount,
                        workerCount,
                        0,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(partitionLimit),
                        task -> {
                            var thread = new Thread(task, "broker-partition");
                            thread.setDaemon(true);
                            return thread;
                        },
                        new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Queues a user task on the lane for {@code key}. The returned future completes with the task's
     * result or with whatever it throws.
     *
     * @throws RejectedExecutionException if the lane already holds {@code queuedTaskLimit} user
     *     tasks, a new lane would exceed {@code partitionLimit}, or the executor is closed; the
     *     task is not queued
     */
    public synchronized <V> CompletableFuture<V> submit(TopicPartition key, Callable<V> action) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(action);
        Lane lane = lane(key);
        if (lane.userTasks.size() >= queuedTaskLimit)
            throw new RejectedExecutionException("Partition queue full");
        CompletableFuture<V> result = new CompletableFuture<>();
        lane.userTasks.addLast(
                () -> {
                    try {
                        result.complete(action.call());
                    } catch (Throwable error) {
                        result.completeExceptionally(error);
                    }
                });
        outstanding++;
        schedule(key, lane);
        return result;
    }

    /**
     * Schedules a control task on the lane for {@code key}. Control tasks do not count against the
     * user-task limit and run before any queued user task, but never interrupt a running one. While
     * a control task is pending, further calls are ignored, so repeated triggers coalesce into one
     * run.
     *
     * @throws RejectedExecutionException if a new lane would exceed {@code partitionLimit} or the
     *     executor is closed
     */
    public synchronized void control(TopicPartition key, Runnable action) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(action);
        Lane lane = lane(key);
        if (lane.control == null) {
            lane.control = action;
            outstanding++;
            schedule(key, lane);
        }
    }

    /**
     * Returns a future that completes the next time no task is queued or running on any lane. It
     * does not stop new submissions.
     */
    public synchronized CompletableFuture<Void> drain() {
        if (outstanding == 0) return CompletableFuture.completedFuture(null);
        var result = new CompletableFuture<Void>();
        drainWaiters.add(result);
        return result;
    }

    /**
     * One reserved barrier per lane, after its queued user/control tasks; callers stop new
     * admission first.
     */
    public synchronized CompletableFuture<Void> afterPending(TopicPartition key, Runnable action) {
        Objects.requireNonNull(action);
        var lane = lane(key);
        if (lane.afterPending != null)
            throw new RejectedExecutionException("Lane barrier already pending");
        var result = new CompletableFuture<Void>();
        lane.afterPending =
                () -> {
                    try {
                        action.run();
                        result.complete(null);
                    } catch (Throwable error) {
                        result.completeExceptionally(error);
                    }
                };
        outstanding++;
        schedule(key, lane);
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
            } else if (!lane.userTasks.isEmpty()) {
                task = lane.userTasks.removeFirst();
            } else {
                task = lane.afterPending;
                lane.afterPending = null;
            }
        }
        try {
            task.run();
        } finally {
            synchronized (this) {
                outstanding--;
                if (lane.control != null
                        || !lane.userTasks.isEmpty()
                        || lane.afterPending != null) {
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

    /**
     * Rejects further submissions, lets already queued tasks finish, and stops the workers. Waits
     * up to 30 seconds for the drain and 30 seconds for thread termination.
     *
     * @throws IllegalStateException if tasks do not drain or workers do not stop in time, or the
     *     calling thread is interrupted
     */
    @Override
    public void close() {
        synchronized (this) {
            closed = true;
        }
        try {
            drain().get(30, TimeUnit.SECONDS);
            workers.shutdown();
            if (!workers.awaitTermination(30, TimeUnit.SECONDS))
                throw new IllegalStateException("Partition workers did not stop");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted closing partition workers", error);
        } catch (java.util.concurrent.ExecutionException
                | java.util.concurrent.TimeoutException error) {
            throw new IllegalStateException("Partition workers did not drain", error);
        }
    }

    private static final class Lane {
        final ArrayDeque<Runnable> userTasks = new ArrayDeque<>();
        Runnable control;
        Runnable afterPending;
        boolean scheduled;
    }
}

package vn.huyqt.logbroker.controller.runtime;

import vn.huyqt.logbroker.controller.consensus.QuorumEvent;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.*;

/**
 * Mutations stay FIFO; independent snapshot serialization uses a bounded lower-priority queue.
 *
 * <p>A single {@code controller-disk} thread runs all controller disk work, so storage objects
 * touched only from submitted tasks need no further locking. Every accepted task first reserves a
 * completion slot on the {@link ControllerLoop}; its result, or failure, is delivered to the loop
 * as {@code DiskDone} or {@code DiskFailed} through that slot. The low-priority queue gets a turn
 * after eight consecutive normal tasks; see {@code docs/controller-storage-v1.md}.
 *
 * <p>Submission is thread-safe; queue state is guarded by this instance's monitor.
 */
public final class OrderedDiskExecutor implements AutoCloseable {
    private record Work(
            DiskToken token,
            Callable<DiskResult> callable,
            ControllerLoop.CompletionTicket ticket) {}

    private final Thread worker;
    private final ControllerLoop loop;
    private final int capacity;
    private final ArrayDeque<Work> normal = new ArrayDeque<>(), low = new ArrayDeque<>();
    private int active, normalTurns;
    private boolean stopping;

    public OrderedDiskExecutor(int capacity, ControllerLoop loop) {
        if (capacity < 1) throw new IllegalArgumentException("Invalid disk capacity");
        this.capacity = capacity;
        this.loop = loop;
        worker = new Thread(this::run, "controller-disk");
        worker.start();
    }

    /**
     * Queues normal FIFO work, such as votes, flushes, truncation and leader-change appends. It may
     * use the completion slots that {@link #submitOrdinary} and {@link #submitLowPriority} leave in
     * reserve.
     *
     * @return false if the executor is stopping, the queue is full, or no completion slot is free;
     *     the work is then not run
     */
    public boolean submit(DiskToken token, Callable<DiskResult> work) {
        return admit(token, work, false);
    }

    /**
     * Queues ordinary append work in FIFO order with other normal work, but refuses it once
     * reserved loop completions reach {@code capacity - min(16, capacity / 2)}, keeping the rest
     * for {@link #submit}; see {@code docs/controller-configuration.md}.
     *
     * @return false if the work was not queued
     */
    public synchronized boolean submitOrdinary(DiskToken token, Callable<DiskResult> work) {
        if (loop.reservedCompletions() >= capacity - Math.min(16, capacity / 2)) return false;
        return admit(token, work, false);
    }

    /**
     * Queues independent work, such as snapshot creation, on the low-priority queue. It is not
     * ordered with normal work and is subject to the same slot reserve as {@link #submitOrdinary}.
     *
     * @return false if the work was not queued
     */
    public boolean submitLowPriority(DiskToken token, Callable<DiskResult> work) {
        return admit(token, work, true);
    }

    private synchronized boolean admit(
            DiskToken token, Callable<DiskResult> work, boolean lowPriority) {
        if (lowPriority && loop.reservedCompletions() >= capacity - Math.min(16, capacity / 2))
            return false;
        if (stopping || normal.size() + low.size() >= capacity) return false;
        // Reserve the loop slot before admitting, so a completed disk operation is never dropped.
        var ticket = loop.reserveCompletion();
        if (ticket == null) return false;
        (lowPriority ? low : normal).addLast(new Work(token, work, ticket));
        active++;
        notifyAll();
        return true;
    }

    private synchronized Work next() throws InterruptedException {
        while (normal.isEmpty() && low.isEmpty() && !stopping) wait();
        if (!low.isEmpty() && (normal.isEmpty() || normalTurns >= 8)) {
            normalTurns = 0;
            return low.removeFirst();
        }
        if (!normal.isEmpty()) {
            normalTurns++;
            return normal.removeFirst();
        }
        return null;
    }

    private void run() {
        try {
            Work work;
            while ((work = next()) != null) {
                QuorumEvent event;
                // An exception is reported as DiskFailed so the reserved slot is still completed.
                try {
                    event = new DiskDone(work.token(), work.callable().call());
                } catch (Exception e) {
                    event = new DiskFailed(work.token(), e.toString());
                }
                try {
                    work.ticket().complete(event);
                } finally {
                    synchronized (this) {
                        active--;
                        notifyAll();
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Waits until every accepted task has run and handed its completion to the loop.
     *
     * @return whether the executor became idle within {@code timeout}
     */
    public synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        long end = System.nanoTime() + timeout.toNanos(), remaining;
        while (active != 0 && (remaining = end - System.nanoTime()) > 0)
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        return active == 0;
    }

    /**
     * Refuses new work, lets the worker finish everything already queued, and waits for it to exit.
     *
     * @return whether the worker exited within {@code timeout}; if not, it keeps running
     */
    public boolean stop(Duration timeout) throws InterruptedException {
        synchronized (this) {
            stopping = true;
            notifyAll();
        }
        worker.join(Math.max(1, timeout.toMillis()));
        return !worker.isAlive();
    }

    /**
     * Stops with a 30-second deadline.
     *
     * @throws IllegalStateException if the worker is still running afterwards or the wait is
     *     interrupted
     */
    @Override
    public void close() {
        try {
            if (!stop(Duration.ofSeconds(30)))
                throw new IllegalStateException("Disk worker still active");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}

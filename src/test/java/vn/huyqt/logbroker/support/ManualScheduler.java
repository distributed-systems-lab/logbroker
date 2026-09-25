package vn.huyqt.logbroker.support;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import vn.huyqt.logbroker.broker.DeadlineScheduler;

public final class ManualScheduler implements DeadlineScheduler {
    private long now;
    private final PriorityQueue<Entry> queue = new PriorityQueue<>(
            Comparator.comparingLong(Entry::deadline));

    @Override public synchronized long nanoTime() { return now; }

    @Override public synchronized Ticket schedule(long deadlineNanos, Runnable action) {
        var entry = new Entry(deadlineNanos, action);
        queue.add(entry);
        return () -> { synchronized (ManualScheduler.this) {
            if (entry.cancelled) return false;
            entry.cancelled = true;
            return true;
        }};
    }

    public synchronized void advance(Duration duration) {
        now = Math.addExact(now, duration.toNanos());
    }

    public void runDue() {
        while (true) {
            Entry entry;
            synchronized (this) {
                if (queue.isEmpty() || queue.peek().deadline > now) return;
                entry = queue.remove();
            }
            if (!entry.cancelled) entry.action.run();
        }
    }

    public synchronized int pending() {
        return (int) queue.stream().filter(entry -> !entry.cancelled).count();
    }

    @Override public synchronized void close() { queue.clear(); }

    private static final class Entry {
        final long deadline;
        final Runnable action;
        volatile boolean cancelled;
        Entry(long deadline, Runnable action) { this.deadline = deadline; this.action = action; }
        long deadline() { return deadline; }
    }
}

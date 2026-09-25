package vn.huyqt.logbroker.support;

import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import vn.huyqt.logbroker.broker.DeadlineScheduler;

public final class ManualScheduler implements DeadlineScheduler {
    private long now;
    private final PriorityQueue<Entry> queue = new PriorityQueue<>(
            Comparator.comparingLong(Entry::deadline));

    @Override public long nanoTime() { return now; }

    @Override public Ticket schedule(long deadlineNanos, Runnable action) {
        var entry = new Entry(deadlineNanos, action);
        queue.add(entry);
        return () -> { if (entry.cancelled) return false; entry.cancelled = true; return true; };
    }

    public void advance(Duration duration) { now = Math.addExact(now, duration.toNanos()); }

    public void runDue() {
        while (!queue.isEmpty() && queue.peek().deadline <= now) {
            var entry = queue.remove();
            if (!entry.cancelled) entry.action.run();
        }
    }

    @Override public void close() { queue.clear(); }

    private static final class Entry {
        final long deadline;
        final Runnable action;
        boolean cancelled;
        Entry(long deadline, Runnable action) { this.deadline = deadline; this.action = action; }
        long deadline() { return deadline; }
    }
}

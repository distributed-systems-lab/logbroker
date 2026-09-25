package vn.huyqt.logbroker.broker;

import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Monotonic deadline scheduling; callbacks must not block on storage I/O. */
public interface DeadlineScheduler extends AutoCloseable {
    long nanoTime();
    Ticket schedule(long deadlineNanos, Runnable action);
    @Override void close();

    interface Ticket { boolean cancel(); }

    static DeadlineScheduler system() {
        return new SystemScheduler();
    }

    final class SystemScheduler implements DeadlineScheduler {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                action -> { var thread = new Thread(action, "broker-deadlines");
                    thread.setDaemon(true); return thread; });

        private SystemScheduler() { executor.setRemoveOnCancelPolicy(true); }

        @Override public long nanoTime() { return System.nanoTime(); }

        @Override public Ticket schedule(long deadlineNanos, Runnable action) {
            Objects.requireNonNull(action);
            long remaining = deadlineNanos - nanoTime();
            var scheduled = executor.schedule(action, Math.max(0, remaining), TimeUnit.NANOSECONDS);
            return () -> scheduled.cancel(false);
        }

        @Override public void close() { executor.shutdownNow(); }
    }
}

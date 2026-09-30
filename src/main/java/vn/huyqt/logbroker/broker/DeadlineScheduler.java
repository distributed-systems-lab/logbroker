package vn.huyqt.logbroker.broker;

import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Monotonic deadline scheduling; callbacks must not block on storage I/O. */
public interface DeadlineScheduler extends AutoCloseable {
    /** Current monotonic time in nanoseconds; comparable only with this scheduler's values. */
    long nanoTime();

    /**
     * Runs {@code action} once at or after {@code deadlineNanos} on {@link #nanoTime()}'s scale.
     * A deadline already in the past runs as soon as possible.
     */
    Ticket schedule(long deadlineNanos, Runnable action);

    @Override
    void close();

    /** Handle for a scheduled action or a registered callback. */
    interface Ticket {
        /**
         * Prevents the action from running if it has not started.
         *
         * @return {@code true} if this call removed a pending action
         */
        boolean cancel();
    }

    /** Returns a scheduler backed by {@link System#nanoTime()} and one daemon thread. */
    static DeadlineScheduler system() {
        return new SystemScheduler();
    }

    /**
     * Single-threaded scheduler; all callbacks run serially on the {@code broker-deadlines}
     * thread. {@link #close()} discards pending actions.
     */
    final class SystemScheduler implements DeadlineScheduler {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1,
                action -> {
                    var thread = new Thread(action, "broker-deadlines");
                    thread.setDaemon(true);
                    return thread;
                });

        private SystemScheduler() {
            executor.setRemoveOnCancelPolicy(true);
        }

        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public Ticket schedule(long deadlineNanos, Runnable action) {
            Objects.requireNonNull(action);
            long remaining = deadlineNanos - nanoTime();
            var scheduled = executor.schedule(action, Math.max(0, remaining), TimeUnit.NANOSECONDS);
            return () -> scheduled.cancel(false);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}

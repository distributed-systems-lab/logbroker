package vn.huyqt.logbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One admitted request's identity, deadline and cancellation hooks.
 *
 * <p>Thread-safe. Cancellation is one-way: once {@link #cancel()} runs, every registered hook
 * runs exactly once and later registrations run immediately.
 */
public final class RequestContext {
    private final long connectionId;
    private final long requestId;
    private final long deadlineNanos;
    private final short version;
    private final List<Runnable> cancellation = new ArrayList<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();

    public RequestContext(long connectionId, long requestId, long deadlineNanos) {
        this(connectionId, requestId, deadlineNanos, (short) 1);
    }

    public RequestContext(long connectionId, long requestId, long deadlineNanos, short version) {
        if (connectionId < 0 || requestId < 0)
            throw new IllegalArgumentException("Negative request identity");
        this.connectionId = connectionId;
        this.requestId = requestId;
        this.deadlineNanos = deadlineNanos;
        this.version = version;
    }
    public short version() { return version; }

    public long connectionId() {
        return connectionId;
    }

    public long requestId() {
        return requestId;
    }

    /** Processing deadline on the {@link DeadlineScheduler#nanoTime()} scale. */
    public long deadlineNanos() {
        return deadlineNanos;
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Registers {@code action} to run when the request is cancelled. If it is already cancelled,
     * {@code action} runs synchronously on the calling thread before this method returns.
     *
     * @return a ticket that unregisters {@code action} if it has not run yet
     */
    public synchronized DeadlineScheduler.Ticket onCancel(Runnable action) {
        Objects.requireNonNull(action);
        if (cancelled.get()) {
            action.run();
            return () -> false;
        }
        cancellation.add(action);
        return () -> {
            synchronized (RequestContext.this) {
                return cancellation.remove(action);
            }
        };
    }

    /** Marks the request cancelled and runs registered hooks on the calling thread; idempotent. */
    public void cancel() {
        if (!cancelled.compareAndSet(false, true))
            return;
        List<Runnable> actions;
        synchronized (this) {
            actions = List.copyOf(cancellation);
            cancellation.clear();
        }
        actions.forEach(Runnable::run);
    }
}

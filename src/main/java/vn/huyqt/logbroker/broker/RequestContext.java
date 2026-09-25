package vn.huyqt.logbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** One admitted request's identity, deadline and cancellation hooks. */
public final class RequestContext {
    private final long connectionId;
    private final long requestId;
    private final long deadlineNanos;
    private final List<Runnable> cancellation = new ArrayList<>();
    private final AtomicBoolean cancelled = new AtomicBoolean();

    public RequestContext(long connectionId, long requestId, long deadlineNanos) {
        if (connectionId < 0 || requestId < 0)
            throw new IllegalArgumentException("Negative request identity");
        this.connectionId = connectionId;
        this.requestId = requestId;
        this.deadlineNanos = deadlineNanos;
    }

    public long connectionId() { return connectionId; }
    public long requestId() { return requestId; }
    public long deadlineNanos() { return deadlineNanos; }
    public boolean isCancelled() { return cancelled.get(); }

    public synchronized DeadlineScheduler.Ticket onCancel(Runnable action) {
        Objects.requireNonNull(action);
        if (cancelled.get()) { action.run(); return () -> false; }
        cancellation.add(action);
        return () -> { synchronized (RequestContext.this) {
            return cancellation.remove(action);
        }};
    }

    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        List<Runnable> actions;
        synchronized (this) {
            actions = List.copyOf(cancellation);
            cancellation.clear();
        }
        actions.forEach(Runnable::run);
    }
}

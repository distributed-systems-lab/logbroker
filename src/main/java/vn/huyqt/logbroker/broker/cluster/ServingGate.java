package vn.huyqt.logbroker.broker.cluster;

import java.util.Objects;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;

/** Admission permission from applied lifecycle decisions only; elapsed time never revokes it. */
public final class ServingGate implements AutoCloseable {
    private Session session;
    private long revision = -1, blockedOffset = -1;
    private boolean allowed, closed;
    private final java.util.concurrent.CopyOnWriteArrayList<Runnable> listeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    public ServingGate(Session session) {
        this.session = Objects.requireNonNull(session);
    }

    /** Binds a newly committed registration and invalidates the old session's permission. */
    public void bind(Session next) {
        synchronized (this) {
            if (closed) throw new IllegalStateException("Serving gate closed");
            Objects.requireNonNull(next);
            if (next.brokerId() != session.brokerId()
                    || !next.storageId().equals(session.storageId())
                    || next.brokerEpoch() < session.brokerEpoch())
                throw new IllegalArgumentException("Invalid replacement session");
            if (next.equals(session)) return;
            session = next;
            revision = blockedOffset = -1;
            allowed = false;
        }
        changed();
    }

    /**
     * Applies a lifecycle decision already covered by an immutable metadata image. Equal or older
     * unfence revisions cannot override a recorded rejection at that boundary.
     */
    public void apply(long imageOffset, long lifecycleRevision, boolean fenced) {
        boolean notify;
        synchronized (this) {
            if (closed
                    || lifecycleRevision < session.brokerEpoch()
                    || imageOffset < lifecycleRevision
                    || lifecycleRevision < revision) return;
            revision = lifecycleRevision;
            boolean previous = allowed;
            if (fenced) {
                blockedOffset = Math.max(blockedOffset, lifecycleRevision);
                allowed = false;
            } else allowed = lifecycleRevision > blockedOffset;
            notify = previous != allowed;
        }
        if (notify) changed();
    }

    /** Ignores a rejection older than an already applied newer decision. */
    public void reject(long stateOffset) {
        boolean notify;
        synchronized (this) {
            if (closed || stateOffset < revision || stateOffset < session.brokerEpoch()) return;
            notify = allowed;
            blockedOffset = Math.max(blockedOffset, stateOffset);
            allowed = false;
        }
        if (notify) changed();
    }

    public synchronized boolean canServe() {
        return !closed && allowed;
    }

    public synchronized Session session() {
        return session;
    }

    public synchronized long blockedOffset() {
        return blockedOffset;
    }

    /**
     * Runs callbacks outside the gate monitor so partition workers can recheck permission safely.
     */
    public vn.huyqt.logbroker.broker.DeadlineScheduler.Ticket onChange(Runnable action) {
        Objects.requireNonNull(action);
        listeners.add(action);
        return () -> listeners.remove(action);
    }

    private void changed() {
        for (var listener : listeners) listener.run();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            allowed = false;
        }
        changed();
    }
}

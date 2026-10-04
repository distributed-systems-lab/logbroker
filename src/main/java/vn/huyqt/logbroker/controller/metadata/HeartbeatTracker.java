package vn.huyqt.logbroker.controller.metadata;

import java.util.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;

/** Loop-owned, volatile liveness. Duplicate sequences never renew contact or authorize serving. */
public final class HeartbeatTracker {
    private static final class Contact {
        final Session session;
        long sequence = -1, receivedAt;

        Contact(Session session, long since) {
            this.session = session;
            receivedAt = since;
        }
    }

    private final long timeout;
    private final Map<Integer, Contact> contacts = new HashMap<>();
    private long started;

    public HeartbeatTracker(long sessionTimeoutNanos) {
        if (sessionTimeoutNanos <= 0) throw new IllegalArgumentException("Invalid session timeout");
        timeout = sessionTimeoutNanos;
    }

    /**
     * Starts a fresh observation window; the previous leader's volatile contact times are
     * discarded.
     */
    public void leaderStarted(long epoch, long now) {
        if (epoch < 0) throw new IllegalArgumentException("Invalid leader epoch");
        contacts.clear();
        started = now;
    }

    /** Registers an authoritative session without making it eligible for assignment. */
    public void observe(Session session) {
        var old = contacts.get(session.brokerId());
        if (old == null || !old.session.equals(session))
            contacts.put(session.brokerId(), new Contact(session, started));
    }

    public boolean record(Session session, long sequence, long now) {
        var c = contacts.get(session.brokerId());
        if (c == null) {
            observe(session);
            c = contacts.get(session.brokerId());
        }
        if (!c.session.equals(session)
                || sequence < 0
                || sequence <= c.sequence
                || now < c.receivedAt) return false;
        c.sequence = sequence;
        c.receivedAt = now;
        return true;
    }

    public boolean eligible(int brokerId, long now) {
        var c = contacts.get(brokerId);
        return c != null && c.sequence >= 0 && now - c.receivedAt < timeout;
    }

    public Set<Integer> expired(long now) {
        var result = new HashSet<Integer>();
        contacts.forEach(
                (id, c) -> {
                    if (now - c.receivedAt >= timeout) result.add(id);
                });
        return Set.copyOf(result);
    }
}

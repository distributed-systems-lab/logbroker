package vn.huyqt.logbroker.broker.cluster;

import java.util.Objects;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;

/** Admission permission from applied lifecycle decisions only; elapsed time never revokes it. */
public final class ServingGate implements AutoCloseable {
    private Session session;
    private long revision=-1, blockedOffset=-1;
    private boolean allowed,closed;
    public ServingGate(Session session) { this.session=Objects.requireNonNull(session); }
    /** Binds a newly committed registration and invalidates the old session's permission. */
    public synchronized void bind(Session next) {
        if(closed) throw new IllegalStateException("Serving gate closed");
        Objects.requireNonNull(next);
        if(next.brokerId()!=session.brokerId() || !next.storageId().equals(session.storageId())
            || next.brokerEpoch()<session.brokerEpoch()) throw new IllegalArgumentException("Invalid replacement session");
        if(next.equals(session)) return;
        session=next; revision=blockedOffset=-1; allowed=false;
    }
    public synchronized void apply(long imageOffset,long lifecycleRevision,boolean fenced) {
        if(closed || lifecycleRevision<session.brokerEpoch() || imageOffset<lifecycleRevision || lifecycleRevision<revision) return;
        revision=lifecycleRevision;
        if(fenced) { blockedOffset=Math.max(blockedOffset,lifecycleRevision); allowed=false; }
        else allowed=lifecycleRevision>blockedOffset;
    }
    /** Ignores a rejection older than an already applied newer decision. */
    public synchronized void reject(long stateOffset) {
        if(closed || stateOffset<revision || stateOffset<session.brokerEpoch()) return;
        blockedOffset=Math.max(blockedOffset,stateOffset); allowed=false;
    }
    public synchronized boolean canServe() { return !closed && allowed; }
    public synchronized Session session() { return session; }
    public synchronized long blockedOffset() { return blockedOffset; }
    @Override public synchronized void close() { closed=true; allowed=false; }
}

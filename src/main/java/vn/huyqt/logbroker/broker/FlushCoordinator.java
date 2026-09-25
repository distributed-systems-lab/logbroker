package vn.huyqt.logbroker.broker;

import java.util.HashSet;
import java.util.Set;

/** Owns flush runtime registrations without duplicating per-partition timers. */
public final class FlushCoordinator implements AutoCloseable {
    private final Set<PartitionRuntime> runtimes = new HashSet<>();

    public synchronized void register(PartitionRuntime runtime) { runtimes.add(runtime); }

    @Override public synchronized void close() {
        for (PartitionRuntime runtime : runtimes) runtime.close();
        runtimes.clear();
    }
}

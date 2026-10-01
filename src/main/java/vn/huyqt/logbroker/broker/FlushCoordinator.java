package vn.huyqt.logbroker.broker;

import java.util.HashSet;
import java.util.Set;

/**
 * Owns flush runtime registrations without duplicating per-partition timers.
 */
public final class FlushCoordinator implements AutoCloseable {
    private final Set<PartitionRuntime> runtimes = new HashSet<>();

    /** Adds {@code runtime} to the set closed by {@link #close()}; registering twice is a no-op. */
    public synchronized void register(PartitionRuntime runtime) {
        runtimes.add(runtime);
    }

    /** Closes every registered runtime without flushing it and forgets the registrations. */
    @Override
    public synchronized void close() {
        for (PartitionRuntime runtime : runtimes)
            runtime.close();
        runtimes.clear();
    }
}

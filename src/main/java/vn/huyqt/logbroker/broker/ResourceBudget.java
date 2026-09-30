package vn.huyqt.logbroker.broker;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe capacity reservation with single-release leases.
 *
 * <p>Callers reserve before queuing or allocating and close the lease on every completion,
 * error and cancellation path. Reservation never blocks; when the budget is exhausted the caller
 * rejects the work, for example with {@code OVERLOADED} or by closing the connection.
 */
public final class ResourceBudget {
    private final long capacity;
    private long used;

    public ResourceBudget(long capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException("Negative capacity");
        this.capacity = capacity;
    }

    /**
     * Reserves {@code amount} units if they fit in the remaining capacity.
     *
     * @return the lease, or empty if the budget cannot cover {@code amount}
     * @throws IllegalArgumentException if {@code amount} is negative
     */
    public synchronized Optional<Lease> reserve(long amount) {
        if (amount < 0)
            throw new IllegalArgumentException("Negative reservation");
        if (amount > capacity - used)
            return Optional.empty();
        used += amount;
        return Optional.of(new Lease(amount));
    }

    public synchronized long used() {
        return used;
    }

    /** A reservation that returns its units exactly once; repeated {@link #close()} is a no-op. */
    public final class Lease implements AutoCloseable {
        private final long amount;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(long amount) {
            this.amount = amount;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                synchronized (ResourceBudget.this) {
                    used -= amount;
                }
            }
        }
    }
}

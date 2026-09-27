package vn.huyqt.logbroker.broker;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Thread-safe capacity reservation with single-release leases. */
public final class ResourceBudget {
    private final long capacity;
    private long used;

    public ResourceBudget(long capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException("Negative capacity");
        this.capacity = capacity;
    }

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

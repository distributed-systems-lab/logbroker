package vn.huyqt.logbroker.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;

/** Immutable client bounds and batching settings. */
public record ClientConfig(InetSocketAddress address, long queuedBytes,
                           int maxInFlight, int targetBatchBytes,
                           Duration linger, Duration requestTimeout) {
    public ClientConfig {
        Objects.requireNonNull(address); Objects.requireNonNull(linger);
        Objects.requireNonNull(requestTimeout);
        if (queuedBytes <= 0 || maxInFlight <= 0 || targetBatchBytes <= 0
                || linger.isNegative() || requestTimeout.isZero() || requestTimeout.isNegative())
            throw new IllegalArgumentException("Invalid client configuration");
    }

    public static ClientConfig defaults(InetSocketAddress address) {
        return new ClientConfig(address, 16L * 1024 * 1024, 32, 64 * 1024,
                Duration.ofMillis(5), Duration.ofSeconds(30));
    }
}

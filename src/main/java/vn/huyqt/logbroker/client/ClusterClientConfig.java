package vn.huyqt.logbroker.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Bounds on the complete cluster operation, rather than independent per-broker budgets. */
public record ClusterClientConfig(
        List<InetSocketAddress> bootstrap,
        UUID expectedClusterId,
        int maxConnections,
        Duration operationTimeout,
        Duration retryBackoff,
        long queuedBytes,
        int maxInFlight) {
    public ClusterClientConfig {
        bootstrap = List.copyOf(bootstrap);
        Objects.requireNonNull(operationTimeout);
        Objects.requireNonNull(retryBackoff);
        if (bootstrap.isEmpty()
                || bootstrap.size() > 32
                || bootstrap.stream().distinct().count() != bootstrap.size()
                || bootstrap.stream().anyMatch(endpoint -> endpoint.getPort() <= 0)
                || expectedClusterId != null && expectedClusterId.equals(new UUID(0, 0))
                || maxConnections < 1
                || maxConnections > 32
                || operationTimeout.isNegative()
                || operationTimeout.isZero()
                || retryBackoff.isNegative()
                || retryBackoff.isZero()
                || queuedBytes <= 0
                || maxInFlight < 1)
            throw new IllegalArgumentException("Invalid cluster client configuration");
        operationTimeout.toNanos();
        retryBackoff.toNanos();
    }

    public static ClusterClientConfig defaults(List<InetSocketAddress> bootstrap) {
        return new ClusterClientConfig(
                bootstrap,
                null,
                32,
                Duration.ofSeconds(30),
                Duration.ofMillis(100),
                16L * 1024 * 1024,
                32);
    }
}

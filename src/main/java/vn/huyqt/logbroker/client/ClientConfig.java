package vn.huyqt.logbroker.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable client bounds and batching settings.
 *
 * <p>{@link BrokerClient} and {@link Producer} each create their own budget of {@code queuedBytes},
 * so a producer and its client can together hold up to twice that amount. Default values follow the
 * Phase 2 design table in {@code docs/superpowers/specs/2026-09-25-broker-phase-2-design.md}.
 *
 * @param address broker endpoint for the single connection of a {@link BrokerClient}
 * @param queuedBytes cap on encoded bytes held by one client or producer before new work is
 *     rejected locally with {@link ClientException.Outcome#NOT_SENT}
 * @param maxInFlight requests awaiting a reply on one {@link BrokerClient}
 * @param targetBatchBytes size at which the producer seals a partition batch
 * @param linger longest time an open producer batch waits for more records before sealing
 * @param requestTimeout client deadline for a request; for the producer it covers batching and
 *     sending, and it is also sent to the broker as the Produce timeout and bounds {@link
 *     Producer#close()}
 */
public record ClientConfig(
        InetSocketAddress address,
        long queuedBytes,
        int maxInFlight,
        int targetBatchBytes,
        Duration linger,
        Duration requestTimeout) {
    public ClientConfig {
        Objects.requireNonNull(address);
        Objects.requireNonNull(linger);
        Objects.requireNonNull(requestTimeout);
        if (queuedBytes <= 0
                || maxInFlight <= 0
                || targetBatchBytes <= 0
                || linger.isNegative()
                || requestTimeout.isZero()
                || requestTimeout.isNegative())
            throw new IllegalArgumentException("Invalid client configuration");
    }

    /** Returns 16 MiB queued bytes, 32 in-flight, 64 KiB batches, 5 ms linger, 30 s timeout. */
    public static ClientConfig defaults(InetSocketAddress address) {
        return new ClientConfig(
                address,
                16L * 1024 * 1024,
                32,
                64 * 1024,
                Duration.ofMillis(5),
                Duration.ofSeconds(30));
    }
}

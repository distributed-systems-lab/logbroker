package vn.huyqt.logbroker.client;

import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Stable unsigned CRC32C key routing.
 *
 * <p>This is the project's own algorithm and makes no compatibility promise with Kafka's
 * partitioner. It is only stable while the partition count is fixed, which holds in Phase 2
 * (see {@code docs/superpowers/specs/2026-09-25-broker-phase-2-design.md}, section 9).
 */
public final class Partitioner {
    private Partitioner() {
    }

    /**
     * Returns the unsigned CRC32C of the raw key bytes modulo {@code partitionCount}. An empty key
     * is hashed like any other key; null keys are routed by {@link Producer} instead.
     *
     * @throws IllegalArgumentException if {@code partitionCount} is not positive
     */
    public static int forKey(byte[] key, int partitionCount) {
        Objects.requireNonNull(key);
        if (partitionCount <= 0)
            throw new IllegalArgumentException("No partitions");
        var crc = new CRC32C();
        crc.update(key, 0, key.length);
        return (int) (crc.getValue() % partitionCount);
    }
}

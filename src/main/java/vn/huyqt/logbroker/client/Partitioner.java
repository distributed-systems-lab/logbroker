package vn.huyqt.logbroker.client;

import java.util.Objects;
import java.util.zip.CRC32C;

/** Stable unsigned CRC32C key routing. */
public final class Partitioner {
    private Partitioner() {
    }

    public static int forKey(byte[] key, int partitionCount) {
        Objects.requireNonNull(key);
        if (partitionCount <= 0)
            throw new IllegalArgumentException("No partitions");
        var crc = new CRC32C();
        crc.update(key, 0, key.length);
        return (int) (crc.getValue() % partitionCount);
    }
}

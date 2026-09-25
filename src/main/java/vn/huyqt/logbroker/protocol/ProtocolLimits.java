package vn.huyqt.logbroker.protocol;

/** Limits checked before allocating protocol payload objects. */
public record ProtocolLimits(int maxFrameBytes, int maxWireBatchBytes,
                             int maxStorageBatchBytes, int maxPartitionEntries,
                             int maxRecordsPerBatch) {
    public ProtocolLimits {
        if (maxFrameBytes < 12 || maxWireBatchBytes < 42 || maxStorageBatchBytes < 50
                || maxWireBatchBytes > maxFrameBytes || maxStorageBatchBytes > maxFrameBytes
                || maxPartitionEntries < 1 || maxRecordsPerBatch < 1)
            throw new IllegalArgumentException("Invalid protocol limits");
    }

    public static ProtocolLimits defaults() {
        return new ProtocolLimits(8 * 1024 * 1024, 1024 * 1024,
                1024 * 1024, 64, 10_000);
    }
}

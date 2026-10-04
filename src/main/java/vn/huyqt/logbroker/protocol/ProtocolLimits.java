package vn.huyqt.logbroker.protocol;

/**
 * Limits checked before allocating protocol payload objects.
 *
 * <p>The lower bounds are the smallest valid frame header (12 bytes), the smallest Fetch batch
 * (8-byte base offset plus a 34-byte wire batch) and the smallest storage batch (50 bytes). Both
 * batch limits must fit in a frame. Size arithmetic is in {@code docs/protocol-v1.md}.
 *
 * @param maxFrameBytes cap on {@code frameLength}, which excludes the length field itself
 * @param maxWireBatchBytes cap on one wire batch including the 8-byte Fetch base offset
 * @param maxStorageBatchBytes cap on the storage batch a wire batch becomes; the broker requires it
 *     not to exceed {@code LogConfig.maxBatchBytes()}
 * @param maxPartitionEntries cap on partition entries in one Produce or Fetch
 * @param maxRecordsPerBatch cap on records in one wire batch
 */
public record ProtocolLimits(
        int maxFrameBytes,
        int maxWireBatchBytes,
        int maxStorageBatchBytes,
        int maxPartitionEntries,
        int maxRecordsPerBatch) {
    public ProtocolLimits {
        if (maxFrameBytes < 12
                || maxWireBatchBytes < 42
                || maxStorageBatchBytes < 50
                || maxWireBatchBytes > maxFrameBytes
                || maxStorageBatchBytes > maxFrameBytes
                || maxPartitionEntries < 1
                || maxRecordsPerBatch < 1)
            throw new IllegalArgumentException("Invalid protocol limits");
    }

    /** Returns the v1 defaults: 8 MiB frames, 1 MiB batches, 64 entries, 10,000 records. */
    public static ProtocolLimits defaults() {
        return new ProtocolLimits(8 * 1024 * 1024, 1024 * 1024, 1024 * 1024, 64, 10_000);
    }
}

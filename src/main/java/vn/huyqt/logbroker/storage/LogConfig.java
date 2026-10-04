package vn.huyqt.logbroker.storage;

/**
 * Size limits for data segments, encoded batches, and sparse index spacing.
 *
 * <p>Requires {@code segmentBytes >= maxBatchBytes >= 50} (50 is the smallest valid encoded batch)
 * and {@code indexIntervalBytes > 0}. When a log is reopened, {@code maxBatchBytes} must still
 * admit every stored batch, otherwise recovery reports corruption. See {@code
 * docs/storage-format-v1.md}.
 *
 * @param segmentBytes maximum data segment size; an append that would exceed it starts a new
 *     segment
 * @param maxBatchBytes upper bound on one encoded batch, header included
 * @param indexIntervalBytes minimum byte distance between sparse index entries
 */
public record LogConfig(long segmentBytes, int maxBatchBytes, int indexIntervalBytes) {
    public LogConfig {
        if (maxBatchBytes < 50 || segmentBytes < maxBatchBytes || indexIntervalBytes <= 0) {
            throw new IllegalArgumentException("Invalid storage limits");
        }
    }

    /** Returns 64 MiB segments, 1 MiB batches and a 4 KiB index interval. */
    public static LogConfig defaults() {
        return new LogConfig(64L * 1024 * 1024, 1024 * 1024, 4096);
    }
}

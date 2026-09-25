package vn.huyqt.logbroker.storage;

public record LogConfig(long segmentBytes, int maxBatchBytes, int indexIntervalBytes) {
    public LogConfig {
        if (maxBatchBytes < 50 || segmentBytes < maxBatchBytes || indexIntervalBytes <= 0) {
            throw new IllegalArgumentException("Invalid storage limits");
        }
    }

    public static LogConfig defaults() {
        return new LogConfig(64L * 1024 * 1024, 1024 * 1024, 4096);
    }
}

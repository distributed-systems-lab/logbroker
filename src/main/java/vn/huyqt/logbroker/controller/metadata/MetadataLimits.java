package vn.huyqt.logbroker.controller.metadata;

/** Admission/decoder bounds, checked before allocation. Counts are not encoded format constants. */
public record MetadataLimits(int maxBrokers, int maxTopics, int maxPartitions, int maxImageBytes) {
    public MetadataLimits {
        if (maxBrokers < 1
                || maxTopics < 1
                || maxPartitions < 1
                || maxImageBytes < 1
                || maxImageBytes > 64 * 1024 * 1024
                || bound(maxBrokers, maxTopics, maxPartitions) > maxImageBytes)
            throw new IllegalArgumentException("Invalid metadata image budget");
    }

    public static MetadataLimits defaults() {
        return new MetadataLimits(32, 128, 1024, 64 * 1024 * 1024);
    }

    /** Conservative upper bound including UUIDs, maximum names/endpoints and RF=1 assignments. */
    public long maxEncodedImageBytes() {
        return bound(maxBrokers, maxTopics, maxPartitions);
    }

    private static long bound(int brokers, int topics, int partitions) {
        return Math.addExact(
                64L,
                Math.addExact(
                        Math.multiplyExact(brokers, 512L),
                        Math.addExact(
                                Math.multiplyExact(topics, 320L),
                                Math.multiplyExact(partitions, 128L))));
    }
}

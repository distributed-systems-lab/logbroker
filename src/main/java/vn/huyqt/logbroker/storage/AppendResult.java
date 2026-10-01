package vn.huyqt.logbroker.storage;

/**
 * The half-open offset range assigned to one appended batch.
 *
 * <p>{@code firstOffset} is the offset of the first record and {@code nextOffset} is exclusive, so
 * the batch holds {@code nextOffset - firstOffset} records. A returned range is not necessarily
 * durable; see {@link PartitionLog#append(java.util.List)}.
 */
public record AppendResult(long firstOffset, long nextOffset) {
    public AppendResult {
        if (firstOffset < 0 || nextOffset <= firstOffset) {
            throw new IllegalArgumentException("Invalid append range");
        }
    }
}

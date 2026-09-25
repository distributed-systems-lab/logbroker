package vn.huyqt.logbroker.storage;

/** The half-open offset range assigned to one appended batch. */
public record AppendResult(long firstOffset, long nextOffset) {
    public AppendResult {
        if (firstOffset < 0 || nextOffset <= firstOffset) {
            throw new IllegalArgumentException("Invalid append range");
        }
    }
}

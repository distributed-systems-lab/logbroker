package vn.huyqt.logbroker.storage;

public record AppendResult(long firstOffset, long nextOffset) {
    public AppendResult {
        if (firstOffset < 0 || nextOffset <= firstOffset) {
            throw new IllegalArgumentException("Invalid append range");
        }
    }
}

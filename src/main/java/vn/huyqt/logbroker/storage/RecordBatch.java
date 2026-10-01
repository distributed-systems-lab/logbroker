package vn.huyqt.logbroker.storage;

import java.util.List;
import java.util.Objects;

/**
 * An immutable batch whose records occupy consecutive offsets.
 *
 * <p>Record {@code i} has offset {@code baseOffset + i}. Batches returned by {@link
 * PartitionLog#read(long, int)} are decoded copies, independent of any file handle.
 */
public final class RecordBatch {
    private final long baseOffset;
    private final List<LogRecord> records;
    private final int encodedSize;

    public RecordBatch(long baseOffset, List<LogRecord> records, int encodedSize) {
        this.records = List.copyOf(Objects.requireNonNull(records, "records"));
        if (baseOffset < 0 || this.records.isEmpty() || encodedSize < 30) {
            throw new IllegalArgumentException("Invalid batch");
        }
        try {
            Math.addExact(baseOffset, this.records.size());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Offset overflow", e);
        }
        this.baseOffset = baseOffset;
        this.encodedSize = encodedSize;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public List<LogRecord> records() {
        return records;
    }

    /** Returns the size of the on-disk storage encoding, header included. */
    public int encodedSize() {
        return encodedSize;
    }

    /** Returns the exclusive end offset, {@code baseOffset + records.size()}. */
    public long nextOffset() {
        return baseOffset + records.size();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof RecordBatch b
                && baseOffset == b.baseOffset
                && encodedSize == b.encodedSize
                && records.equals(b.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseOffset, records, encodedSize);
    }
}

package vn.huyqt.logbroker.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A record with caller-supplied timestamp and defensively copied payload bytes.
 *
 * <p>Immutable. A {@code null} key or value is distinct from an empty array and survives encoding.
 * The timestamp is opaque to storage and does not affect log order. Headers keep their order,
 * including duplicate keys.
 */
public final class LogRecord {
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;
    private final List<RecordHeader> headers;

    public LogRecord(long timestamp, byte[] key, byte[] value, List<RecordHeader> headers) {
        this.timestamp = timestamp;
        this.key = key == null ? null : key.clone();
        this.value = value == null ? null : value.clone();
        this.headers = List.copyOf(Objects.requireNonNull(headers, "headers"));
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        return key == null ? null : key.clone();
    }

    public byte[] value() {
        return value == null ? null : value.clone();
    }

    public List<RecordHeader> headers() {
        return headers;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LogRecord r
                && timestamp == r.timestamp
                && Arrays.equals(key, r.key)
                && Arrays.equals(value, r.value)
                && headers.equals(r.headers);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return 31 * result + headers.hashCode();
    }
}

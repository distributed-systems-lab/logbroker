package vn.huyqt.logbroker.storage;

import java.util.Arrays;
import java.util.Objects;

public final class RecordHeader {
    private final String key;
    private final byte[] value;

    public RecordHeader(String key, byte[] value) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value == null ? null : value.clone();
    }

    public String key() { return key; }
    public byte[] value() { return value == null ? null : value.clone(); }

    @Override public boolean equals(Object other) {
        return other instanceof RecordHeader h && key.equals(h.key) && Arrays.equals(value, h.value);
    }

    @Override public int hashCode() {
        return 31 * key.hashCode() + Arrays.hashCode(value);
    }
}

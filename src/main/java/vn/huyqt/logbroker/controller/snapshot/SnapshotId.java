package vn.huyqt.logbroker.controller.snapshot;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

public record SnapshotId(long endOffset,long lastEpoch,UUID contentId) {
    public SnapshotId {
        Objects.requireNonNull(contentId);
        if(endOffset<0||lastEpoch<0||contentId.equals(new UUID(0,0)))throw new IllegalArgumentException("Invalid snapshot identity");
    }
    public void writeTo(ByteBuffer out) {
        out.putLong(endOffset).putLong(lastEpoch).putLong(contentId.getMostSignificantBits()).putLong(contentId.getLeastSignificantBits());
    }
    public static SnapshotId readFrom(ByteBuffer in) { return new SnapshotId(in.getLong(),in.getLong(),new UUID(in.getLong(),in.getLong())); }
}

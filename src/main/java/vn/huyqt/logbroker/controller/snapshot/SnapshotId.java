package vn.huyqt.logbroker.controller.snapshot;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable identity of one published metadata snapshot.
 *
 * <p>{@code endOffset} is the exclusive log boundary the image covers, {@code lastEpoch} is the
 * epoch of the last entry before that boundary, and {@code contentId} names the snapshot file. The
 * same 32-byte layout is used on the wire and in the state journal; see {@code
 * docs/controller-protocol-v1.md}.
 */
public record SnapshotId(long endOffset, long lastEpoch, UUID contentId) {
    public SnapshotId {
        Objects.requireNonNull(contentId);
        if (endOffset < 0 || lastEpoch < 0 || contentId.equals(new UUID(0, 0)))
            throw new IllegalArgumentException("Invalid snapshot identity");
    }

    /** Writes the 32-byte encoding at the buffer's position. */
    public void writeTo(ByteBuffer out) {
        out.putLong(endOffset)
                .putLong(lastEpoch)
                .putLong(contentId.getMostSignificantBits())
                .putLong(contentId.getLeastSignificantBits());
    }

    /**
     * Reads a 32-byte encoding from the buffer's position.
     *
     * @throws java.nio.BufferUnderflowException if fewer than 32 bytes remain
     * @throws IllegalArgumentException if the decoded fields are not a valid identity
     */
    public static SnapshotId readFrom(ByteBuffer in) {
        return new SnapshotId(in.getLong(), in.getLong(), new UUID(in.getLong(), in.getLong()));
    }
}

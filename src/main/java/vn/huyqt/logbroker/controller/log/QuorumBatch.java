package vn.huyqt.logbroker.controller.log;

import java.util.List;

/**
 * One leader batch of quorum entries at consecutive offsets from {@code baseOffset}.
 *
 * <p>A batch never mixes epochs, and replicas store it with the leader's offsets and boundaries
 * unchanged, so every append, durable, commit and apply offset is a batch boundary. The entry list
 * is copied and immutable.
 */
public record QuorumBatch(long baseOffset, List<QuorumEntry> entries) {
    public QuorumBatch {
        entries = List.copyOf(entries);
        if (baseOffset < 0 || entries.isEmpty())
            throw new IllegalArgumentException("Invalid batch");
        try {
            Math.addExact(baseOffset, entries.size());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Offset overflow", e);
        }
        long epoch = entries.getFirst().epoch();
        if (entries.stream().anyMatch(e -> e.epoch() != epoch))
            throw new IllegalArgumentException("Mixed batch epoch");
    }

    /** Returns the exclusive end offset of this batch. */
    public long nextOffset() {
        return Math.addExact(baseOffset, entries.size());
    }
}

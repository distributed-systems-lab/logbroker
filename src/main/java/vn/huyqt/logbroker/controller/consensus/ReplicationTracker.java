package vn.huyqt.logbroker.controller.consensus;

import vn.huyqt.logbroker.controller.log.EpochIndex;

import java.util.*;

/**
 * Remote durable matches, scoped to one leadership epoch. Local durability is supplied separately.
 *
 * <p>Owned by {@link QuorumStateMachine} and confined to its event loop.
 */
public final class ReplicationTracker {
    private long epoch;
    private final Map<Integer, Long> matches = new HashMap<>();

    /** Starts a new leadership epoch and forgets all matches from the previous one. */
    public void reset(long epoch) {
        this.epoch = epoch;
        matches.clear();
    }

    /**
     * Records that {@code voter} durably holds the leader's prefix up to {@code end}, if {@code
     * (end, lastEpoch)} is a retained boundary of {@code index}. Matches never move backwards.
     */
    public void confirm(int voter, long end, long lastEpoch, EpochIndex index) {
        try {
            if (index.positionAt(end).lastEpoch() == lastEpoch)
                matches.merge(voter, end, Math::max);
        } catch (IllegalArgumentException ignored) {
            /* An offset alone is not matching-prefix evidence. */
        }
    }

    /**
     * Returns the highest boundary that may be committed, or 0 if none: it must be locally durable,
     * durably matched by a remote voter, and end a batch of the current leader {@code epoch}.
     * Earlier-epoch batches below it commit indirectly.
     */
    public long committable(long localDurable, long epoch, EpochIndex index) {
        if (this.epoch != epoch) return 0;
        // With the fixed three-voter set, the leader plus the best remote match form a majority.
        long remote = matches.values().stream().mapToLong(Long::longValue).max().orElse(0);
        long ceiling = Math.min(localDurable, remote);
        return index.boundaries().stream()
                .filter(p -> p.endOffset() <= ceiling && p.lastEpoch() == epoch)
                .mapToLong(EpochIndex.LogPosition::endOffset)
                .max()
                .orElse(0);
    }

    public Map<Integer, Long> matches() {
        return Map.copyOf(matches);
    }
}

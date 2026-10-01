package vn.huyqt.logbroker.controller.log;

import java.util.*;

/**
 * Immutable batch-boundary index handed from disk worker to consensus.
 *
 * <p>Maps every exclusive batch end at or after the logical origin to the epoch of the batch that
 * ends there. The origin itself maps to the snapshot (or initial) epoch, so a node whose log is
 * empty after a snapshot still has a position to advertise. Offsets that are not retained batch
 * boundaries have no epoch and are rejected rather than approximated.
 *
 * <p>Instances never change after construction and may be shared across threads.
 */
public final class EpochIndex {
  /** A batch boundary: exclusive end offset and the epoch of the batch that ends there. */
  public record LogPosition(long endOffset, long lastEpoch) {}

  private final NavigableMap<Long, Long> epochsByEnd;
  private final long start;

  /**
   * Builds the index for a log whose logical origin is {@code start}.
   *
   * @param snapshotEpoch epoch of the last entry before {@code start}, or 0 for a fresh log
   * @param batches retained batches from {@code start}, in offset order
   * @throws IllegalArgumentException if the batches are not contiguous from {@code start} or their
   *     epochs decrease
   */
  public EpochIndex(long start, long snapshotEpoch, List<QuorumBatch> batches) {
    if (start < 0 || snapshotEpoch < 0) throw new IllegalArgumentException("Invalid epoch origin");
    this.start = start;
    var boundaries = new TreeMap<Long, Long>();
    boundaries.put(start, snapshotEpoch);
    long end = start, epoch = snapshotEpoch;
    for (var batch : batches) {
      if (batch.baseOffset() != end || batch.entries().getFirst().epoch() < epoch)
        throw new IllegalArgumentException("Noncontiguous epoch index");
      epoch = batch.entries().getFirst().epoch();
      end = batch.nextOffset();
      boundaries.put(end, epoch);
    }
    epochsByEnd = Collections.unmodifiableNavigableMap(boundaries);
  }

  /**
   * Returns the position at an exact retained boundary.
   *
   * @throws IllegalArgumentException if {@code end} is not a retained batch boundary
   */
  public LogPosition positionAt(long end) {
    Long epoch = epochsByEnd.get(end);
    if (epoch == null) throw new IllegalArgumentException("Not a retained batch boundary");
    return new LogPosition(end, epoch);
  }

  /**
   * Returns the largest retained boundary whose batch belongs to {@code epoch}.
   *
   * @throws IllegalArgumentException if no retained boundary carries {@code epoch}
   */
  public long endOfEpoch(long epoch) {
    return epochsByEnd.descendingMap().entrySet().stream()
        .filter(e -> e.getValue() == epoch)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Epoch not retained"))
        .getKey();
  }

  /**
   * Returns the largest boundary at or below both {@code followerEnd} and {@link #end()} whose
   * epoch does not exceed {@code followerEpoch}. The leader sends it as a divergence hint.
   *
   * @throws IllegalArgumentException if the answer would lie before the logical origin; the
   *     follower then needs a snapshot instead
   */
  public LogPosition commonPrefix(long followerEnd, long followerEpoch) {
    if (followerEnd < start) throw new IllegalArgumentException("Prefix requires snapshot");
    for (var entry :
        epochsByEnd.headMap(Math.min(followerEnd, end()), true).descendingMap().entrySet())
      if (entry.getValue() <= followerEpoch)
        return new LogPosition(entry.getKey(), entry.getValue());
    throw new IllegalArgumentException("Prefix requires snapshot");
  }

  /** Logical origin: the snapshot end, or the log start when there is no recovery snapshot. */
  public long start() {
    return start;
  }

  public long end() {
    return epochsByEnd.lastKey();
  }

  /** Returns all retained boundaries in ascending offset order, starting with the origin. */
  public List<LogPosition> boundaries() {
    return epochsByEnd.entrySet().stream()
        .map(e -> new LogPosition(e.getKey(), e.getValue()))
        .toList();
  }
}

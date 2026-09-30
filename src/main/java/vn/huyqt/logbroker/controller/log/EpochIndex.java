package vn.huyqt.logbroker.controller.log;

import java.util.*;

/** Immutable batch-boundary index handed from disk worker to consensus. */
public final class EpochIndex {
  public record LogPosition(long endOffset, long lastEpoch) {}

  private final NavigableMap<Long, Long> epochsByEnd;
  private final long start;

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

  public LogPosition positionAt(long end) {
    Long epoch = epochsByEnd.get(end);
    if (epoch == null) throw new IllegalArgumentException("Not a retained batch boundary");
    return new LogPosition(end, epoch);
  }

  public long endOfEpoch(long epoch) {
    return epochsByEnd.descendingMap().entrySet().stream()
        .filter(e -> e.getValue() == epoch)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Epoch not retained"))
        .getKey();
  }

  public LogPosition commonPrefix(long followerEnd, long followerEpoch) {
    if (followerEnd < start) throw new IllegalArgumentException("Prefix requires snapshot");
    for (var entry :
        epochsByEnd.headMap(Math.min(followerEnd, end()), true).descendingMap().entrySet())
      if (entry.getValue() <= followerEpoch)
        return new LogPosition(entry.getKey(), entry.getValue());
    throw new IllegalArgumentException("Prefix requires snapshot");
  }

  public long start() {
    return start;
  }

  public long end() {
    return epochsByEnd.lastKey();
  }

  public List<LogPosition> boundaries() {
    return epochsByEnd.entrySet().stream()
        .map(e -> new LogPosition(e.getKey(), e.getValue()))
        .toList();
  }
}

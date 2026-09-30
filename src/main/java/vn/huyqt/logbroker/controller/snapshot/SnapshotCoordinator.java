package vn.huyqt.logbroker.controller.snapshot;

import java.util.*;
import java.util.function.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.metadata.*;

/** Loop-owned trigger; a single immutable bounded image crosses to the disk worker. */
public final class SnapshotCoordinator implements AutoCloseable {
  private final long threshold;
  private final UUID generation;
  private final LongSupplier epoch;
  private final Consumer<QuorumEffect.DiskEffect> submit;
  private final Consumer<SnapshotId> published;
  private final Supplier<List<SnapshotId>> retained;
  private long sequence, baseline, capturedBytes, retainedBytes, latestBytes, latestEpoch;
  private MetadataImage latest;
  private DiskToken creation;
  private boolean closed;

  public SnapshotCoordinator(
      long threshold,
      UUID generation,
      LongSupplier epoch,
      Consumer<QuorumEffect.DiskEffect> submit,
      Consumer<SnapshotId> published,
      Supplier<List<SnapshotId>> retained) {
    if (threshold <= 0) throw new IllegalArgumentException("Invalid snapshot threshold");
    this.threshold = threshold;
    this.generation = generation;
    this.epoch = epoch;
    this.submit = submit;
    this.published = published;
    this.retained = retained;
  }

  public void onApplied(MetadataImage image, long lastEpoch, long appendedBytes) {
    if (closed) return;
    if (appendedBytes < latestBytes)
      throw new IllegalArgumentException("Append byte counter decreased");
    latest = image;
    latestEpoch = lastEpoch;
    latestBytes = appendedBytes;
    if (creation != null || appendedBytes - baseline < threshold) return;
    // v1 has at most 128 topics: the entire encoded image fits in one 256 KiB slice.
    retainedBytes = 114L + image.topics().stream().mapToLong(t -> 32L + t.name().length()).sum();
    if (retainedBytes > 256 * 1024)
      throw new IllegalArgumentException("Snapshot image exceeds one bounded slice");
    capturedBytes = appendedBytes;
    creation = token();
    submit.accept(new QuorumEffect.CreateSnapshot(creation, image, lastEpoch));
  }

  public void onCreated(SnapshotId id) {
    if (closed || creation == null) return;
    creation = null;
    retainedBytes = 0;
    baseline = capturedBytes;
    published.accept(id);
    var ids = retained.get();
    if (ids.size() == 2)
      submit.accept(new QuorumEffect.RetainSnapshotPrefix(token(), ids.get(1).endOffset()));
    if (latest != null) onApplied(latest, latestEpoch, latestBytes);
  }

  public boolean onCompletion(DiskDone done) {
    if (creation != null && creation.equals(done.token())) {
      if (done.result() instanceof DiskResult.SnapshotCreated result) onCreated(result.id());
      else if (done.result() instanceof DiskResult.Overloaded) {
        creation = null;
        retainedBytes = 0;
      }
      return true;
    }
    return false;
  }

  private DiskToken token() {
    return new DiskToken(Long.MAX_VALUE - ++sequence, epoch.getAsLong(), generation);
  }

  public long retainedBytes() {
    return retainedBytes;
  }

  @Override
  public void close() {
    closed = true;
    latest = null;
    retainedBytes = 0;
    creation = null;
  }
}

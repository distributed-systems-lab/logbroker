package vn.huyqt.logbroker.controller.snapshot;

import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.metadata.*;

import java.util.*;
import java.util.function.*;

/**
 * Loop-owned trigger; a single immutable bounded image crosses to the disk worker.
 *
 * <p>Requests a local snapshot once the appended log bytes since the last snapshot reach the
 * threshold, with at most one creation in flight. After a creation completes and two snapshots are
 * retained, it requests prefix retention up to the older one. One instance serves one log
 * generation and is replaced when a new generation is installed. Not thread-safe; all methods run
 * on the controller loop.
 */
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

    /**
     * @param threshold appended log bytes between snapshots
     * @param generation log generation stamped on every disk token this instance creates
     * @param epoch current epoch, stamped on disk tokens
     * @param submit receives snapshot creation and prefix retention effects
     * @param published notified with each newly created snapshot
     * @param retained current retained snapshot set, newest first
     */
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

    /**
     * Records the latest applied image and submits a creation if the threshold is reached and none
     * is in flight. Ignored after {@link #close}.
     *
     * @param lastEpoch epoch of the last entry covered by {@code image}
     * @param appendedBytes running counter of appended log bytes; must never decrease
     * @throws IllegalArgumentException if {@code appendedBytes} decreased, or the image is too
     *     large to fit in the snapshot envelope budget
     */
    public void onApplied(MetadataImage image, long lastEpoch, long appendedBytes) {
        if (closed) return;
        if (appendedBytes < latestBytes)
            throw new IllegalArgumentException("Append byte counter decreased");
        latest = image;
        latestEpoch = lastEpoch;
        latestBytes = appendedBytes;
        if (creation != null || appendedBytes - baseline < threshold) return;
        // Charge the whole immutable image; transport reads it in independently bounded chunks.
        retainedBytes = 102L + MetadataImageCodec.encodedSize(image);
        if (retainedBytes > 64L * 1024 * 1024)
            throw new IllegalArgumentException("Snapshot image exceeds envelope budget");
        capturedBytes = appendedBytes;
        creation = token();
        submit.accept(new QuorumEffect.CreateSnapshot(creation, image, lastEpoch));
    }

    /**
     * Completes the in-flight creation: resets the byte baseline to the captured point, announces
     * {@code id}, requests prefix retention if two snapshots are retained, and re-evaluates the
     * latest image in case the threshold was crossed again meanwhile.
     */
    public void onCreated(SnapshotId id) {
        if (closed || creation == null) return;
        creation = null;
        retainedBytes = 0;
        baseline = capturedBytes;
        published.accept(id);
        var ids = retained.get();
        // The older of the two retained snapshots becomes the recovery base, so only log before it
        // can be dropped; see docs/controller-storage-v1.md.
        if (ids.size() == 2)
            submit.accept(new QuorumEffect.RetainSnapshotPrefix(token(), ids.get(1).endOffset()));
        if (latest != null) onApplied(latest, latestEpoch, latestBytes);
    }

    /**
     * Handles the completion of this coordinator's in-flight creation. An {@code Overloaded} result
     * clears it so a later {@link #onApplied} can retry.
     *
     * @return whether {@code done} belonged to the in-flight creation
     */
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

    // Counts down from Long.MAX_VALUE while the state machine's disk tokens count up, so the two
    // operation ID ranges do not meet in practice.
    private DiskToken token() {
        return new DiskToken(Long.MAX_VALUE - ++sequence, epoch.getAsLong(), generation);
    }

    /** Estimated encoded size of the image held by the in-flight creation, or 0 if none. */
    public long retainedBytes() {
        return retainedBytes;
    }

    /** Stops triggering and forgets the in-flight creation; later completions are ignored. */
    @Override
    public void close() {
        closed = true;
        latest = null;
        retainedBytes = 0;
        creation = null;
    }
}

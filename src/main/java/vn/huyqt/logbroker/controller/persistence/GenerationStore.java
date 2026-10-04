package vn.huyqt.logbroker.controller.persistence;

import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.snapshot.*;
import vn.huyqt.logbroker.storage.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/**
 * Journal references decide the active generation; directory discovery cannot invent committed
 * state.
 *
 * <p>A generation is one metadata log directory under {@code generations/<uuid>/log}, optionally
 * based on a snapshot. After format, new generations are only created by snapshot install. Suffix
 * truncation and prefix retention happen within the active generation and follow an intent/done
 * protocol in the {@link StateJournal}, so {@link #open} can complete an operation that a crash
 * interrupted.
 *
 * <p>Mutating methods are not thread-safe and must be serialized by the caller. Only {@link
 * #generation()} may be read from other threads.
 */
public final class GenerationStore implements AutoCloseable {
    /** Thrown when the install fence closes before the new generation is published. */
    public static final class InstallCancelled extends IOException {
        public InstallCancelled() {
            super("Snapshot install fence expired");
        }
    }

    private final QuorumStateStore state;
    private final DurableFiles files;
    private final LogConfig config;
    private volatile UUID generation;
    private long start, committed;
    private QuorumLog log;
    private SnapshotId baseSnapshot;

    private GenerationStore(QuorumStateStore state, DurableFiles files, LogConfig config) {
        this.state = state;
        this.files = files;
        this.config = config;
    }

    /**
     * Replays the journal to find the active generation, completes any pending truncate or prefix
     * intent, and opens that generation's log.
     *
     * @throws IOException if the journal is inconsistent or the committed offset is not a retained
     *     batch boundary
     */
    public static GenerationStore open(QuorumStateStore state, DurableFiles files, LogConfig config)
            throws IOException {
        var store = new GenerationStore(state, files, config);
        store.recover();
        return store;
    }

    // Replay intents before opening the log so a crash between journal and file mutations is
    // recoverable.
    private void recover() throws IOException {
        Long truncate = null;
        long truncateSequence = 0;
        Long prefix = null;
        long prefixSequence = 0;
        for (var frame : state.journal().frames()) {
            var in = ByteBuffer.wrap(frame.payload());
            try {
                switch (frame.type()) {
                    case StateJournal.GENERATION -> {
                        UUID next = new UUID(in.getLong(), in.getLong());
                        long nextStart = in.getLong();
                        byte present = in.get();
                        if (present != 0 && present != 1)
                            throw new IOException("Invalid generation snapshot flag");
                        SnapshotId snapshot = present == 1 ? SnapshotId.readFrom(in) : null;
                        if (nextStart < 0
                                || snapshot != null && snapshot.endOffset() < nextStart
                                || in.hasRemaining())
                            throw new IOException("Invalid generation record");
                        // A different UUID switches generations: intents from the old one no longer
                        // apply.
                        // The same UUID means prefix retention moved the recovery base forward.
                        if (!next.equals(generation)) {
                            committed = snapshot == null ? nextStart : snapshot.endOffset();
                            truncate = null;
                            prefix = null;
                        } else if (snapshot != null) {
                            if (baseSnapshot != null
                                    && snapshot.endOffset() < baseSnapshot.endOffset())
                                throw new IOException("Recovery base regression");
                            committed = Math.max(committed, snapshot.endOffset());
                        }
                        generation = next;
                        start = nextStart;
                        baseSnapshot = snapshot;
                    }
                        // Boundary records carry their generation UUID; records for other
                        // generations are
                        // stale and ignored.
                    case StateJournal.COMMIT -> {
                        UUID id = new UUID(in.getLong(), in.getLong());
                        long end = in.getLong();
                        if (in.hasRemaining()) throw new IOException("Invalid commit length");
                        if (id.equals(generation)) {
                            if (end < committed) throw new IOException("Commit regression");
                            committed = end;
                        }
                    }
                    case StateJournal.TRUNCATE_INTENT -> {
                        UUID id = new UUID(in.getLong(), in.getLong());
                        long end = in.getLong();
                        if (in.hasRemaining()) throw new IOException("Invalid truncate intent");
                        if (id.equals(generation)) {
                            truncate = end;
                            truncateSequence = frame.sequence();
                        }
                    }
                        // DONE records reference their intent by journal sequence, not by value.
                    case StateJournal.TRUNCATE_DONE -> {
                        if (in.getLong() == truncateSequence) truncate = null;
                        if (in.hasRemaining()) throw new IOException("Invalid truncate completion");
                    }
                    case StateJournal.PREFIX_INTENT -> {
                        UUID id = new UUID(in.getLong(), in.getLong());
                        long value = in.getLong();
                        if (in.hasRemaining()) throw new IOException("Invalid prefix intent");
                        if (id.equals(generation)) {
                            prefix = value;
                            start = value;
                            prefixSequence = frame.sequence();
                        }
                    }
                    case StateJournal.PREFIX_DONE -> {
                        if (in.getLong() == prefixSequence) prefix = null;
                        if (in.hasRemaining()) throw new IOException("Invalid prefix completion");
                    }
                    default -> {}
                }
            } catch (java.nio.BufferUnderflowException e) {
                throw new IOException("Truncated generation journal payload", e);
            }
        }
        if (generation == null) throw new IOException("No published generation");
        Path directory = directory();
        // Finish an interrupted prefix removal. Deleting segments whose base is below the intent is
        // idempotent, so it is safe to repeat after a crash.
        if (prefix != null) {
            if (prefix > committed) throw new IOException("Prefix intent exceeds commit");
            try (var paths = Files.list(directory)) {
                for (var path : paths.toList()) {
                    String name = path.getFileName().toString();
                    if (name.matches("[0-9]{20}\\.(log|index)")
                            && Long.parseLong(name.substring(0, 20)) < prefix) Files.delete(path);
                }
            }
            files.syncDirectory(directory);
            state.journal()
                    .append(
                            StateJournal.PREFIX_DONE,
                            ByteBuffer.allocate(8).putLong(prefixSequence).array());
        }
        // Finish an interrupted suffix truncation before the log is opened normally.
        if (truncate != null) {
            LogIntentRecovery.truncate(
                    directory, config, start, committed, truncate, files::syncDirectory);
            state.journal()
                    .append(
                            StateJournal.TRUNCATE_DONE,
                            ByteBuffer.allocate(8).putLong(truncateSequence).array());
        }
        long logicalStart = baseSnapshot == null ? start : baseSnapshot.endOffset(),
                lastEpoch = baseSnapshot == null ? 0 : baseSnapshot.lastEpoch();
        if (baseSnapshot != null)
            new SnapshotStore(state.root(), state.identity(), files, state, 64 * 1024 * 1024)
                    .load(baseSnapshot);
        log =
                QuorumLog.open(
                        directory, config, start, committed, logicalStart, lastEpoch, false, files);
        try {
            log.epochs().positionAt(committed);
        } catch (IllegalArgumentException e) {
            log.close();
            throw new IOException("Commit is not a retained batch boundary", e);
        }
    }

    /**
     * Durably records {@code end} as the committed offset of the active generation. A repeated
     * checkpoint of the current offset is a no-op.
     *
     * @throws IllegalArgumentException if {@code end} moves backwards, exceeds the durable log end,
     *     or is not a batch boundary
     */
    public void checkpointCommit(long end) throws IOException {
        if (end < committed || end > log.durableEnd())
            throw new IllegalArgumentException("Invalid committed boundary");
        log.epochs().positionAt(end);
        if (end == committed) return;
        state.journal().append(StateJournal.COMMIT, encodeBoundary(end));
        committed = end;
    }

    /**
     * Removes the uncommitted log suffix from {@code end}, for example after a new leader's log
     * diverges from this one. The intent is journaled first so a crash mid-truncation is completed
     * on recovery.
     *
     * @throws IllegalArgumentException if {@code end} is below the committed offset or not a batch
     *     boundary
     */
    public void truncate(long end) throws IOException {
        if (end < committed) throw new IllegalArgumentException("Truncate crosses commit");
        log.epochs().positionAt(end);
        long intent = state.journal().append(StateJournal.TRUNCATE_INTENT, encodeBoundary(end));
        log.truncate(end, committed);
        state.journal()
                .append(StateJournal.TRUNCATE_DONE, ByteBuffer.allocate(8).putLong(intent).array());
    }

    /**
     * Makes the older of the two retained snapshots the recovery base and deletes sealed log
     * segments that lie entirely before it. The segment containing the snapshot boundary is kept.
     *
     * @param olderSnapshotEnd end offset of the older retained snapshot; must be committed
     * @throws IllegalArgumentException if exactly two snapshots are not retained, the offset does
     *     not match the older one, or its epoch disagrees with the log
     */
    public void retainPrefix(long olderSnapshotEnd) throws IOException {
        var snapshots =
                new SnapshotStore(state.root(), state.identity(), files, state, 64 * 1024 * 1024);
        if (snapshots.retained().size() != 2
                || snapshots.retained().get(1).endOffset() != olderSnapshotEnd
                || olderSnapshotEnd > committed)
            throw new IllegalArgumentException("Retention requires two committed snapshots");
        var base = snapshots.retained().get(1);
        log.epochs().positionAt(base.endOffset());
        if (base.endOffset() < log.start()
                || log.epochs().positionAt(base.endOffset()).lastEpoch() != base.lastEpoch())
            throw new IllegalArgumentException("Snapshot epoch mismatch");
        // The new recovery base is durable before any bytes required by the old base disappear.
        state.journal().append(StateJournal.GENERATION, encodeGeneration(generation, start, base));
        baseSnapshot = base;
        long nextStart = log.storage().prefixStartAfter(olderSnapshotEnd);
        long intent = state.journal().append(StateJournal.PREFIX_INTENT, encodeBoundary(nextStart));
        start = nextStart;
        log.advanceStart(base.endOffset(), base.lastEpoch());
        log.storage().deleteSegmentsBefore(olderSnapshotEnd);
        log.refresh();
        state.journal()
                .append(StateJournal.PREFIX_DONE, ByteBuffer.allocate(8).putLong(intent).array());
    }

    /**
     * Encodes a {@link StateJournal#GENERATION} payload.
     *
     * @param start physical base offset of the first retained segment, not the logical origin
     * @param snapshot recovery base snapshot, or {@code null} for a log that starts at {@code
     *     start}
     */
    public static byte[] encodeGeneration(UUID id, long start, SnapshotId snapshot) {
        var out =
                ByteBuffer.allocate(snapshot == null ? 25 : 57)
                        .putLong(id.getMostSignificantBits())
                        .putLong(id.getLeastSignificantBits())
                        .putLong(start)
                        .put((byte) (snapshot == null ? 0 : 1));
        if (snapshot != null) snapshot.writeTo(out);
        return out.array();
    }

    /**
     * Returns the snapshot referenced by the latest GENERATION record, or {@code null} if it has
     * none. {@link SnapshotStore} treats it as referenced so the recovery base is never deleted.
     */
    public static SnapshotId snapshotReference(QuorumStateStore state) throws IOException {
        SnapshotId result = null;
        for (var frame : state.journal().frames())
            if (frame.type() == StateJournal.GENERATION) {
                try {
                    var in = ByteBuffer.wrap(frame.payload());
                    in.getLong();
                    in.getLong();
                    in.getLong();
                    byte present = in.get();
                    if (present != 0 && present != 1)
                        throw new IOException("Invalid generation snapshot flag");
                    result = present == 1 ? SnapshotId.readFrom(in) : null;
                    if (in.hasRemaining()) throw new IOException("Invalid generation payload");
                } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
                    throw new IOException("Invalid generation snapshot reference", e);
                }
            }
        return result;
    }

    /**
     * Rebuilds the metadata image from the base snapshot plus committed log batches. Uncommitted
     * batches are never applied.
     *
     * @throws IOException if the log is missing committed data
     */
    public MetadataImage recoveredImage() throws IOException {
        MetadataImage base =
                baseSnapshot == null
                        ? null
                        : new SnapshotStore(
                                        state.root(),
                                        state.identity(),
                                        files,
                                        state,
                                        64 * 1024 * 1024)
                                .load(baseSnapshot);
        return replayImage(
                MetadataLimits.defaults(),
                (short) (base == null || base.metadataVersion() == 1 ? 1 : 2),
                base);
    }

    /**
     * Replays a formatted cluster with explicit schema and capacity; never guesses from log bytes.
     */
    public MetadataImage recoveredImage(MetadataLimits limits, short schema) throws IOException {
        MetadataImage base =
                baseSnapshot == null
                        ? null
                        : new SnapshotStore(
                                        state.root(),
                                        state.identity(),
                                        files,
                                        state,
                                        64 * 1024 * 1024,
                                        limits)
                                .load(baseSnapshot);
        return replayImage(limits, schema, base);
    }

    private MetadataImage replayImage(MetadataLimits limits, short schema, MetadataImage base)
            throws IOException {
        var metadata = new MetadataStateMachine(limits, schema);
        if (base != null) metadata.restore(base);
        while (metadata.image().appliedOffset() < committed) {
            var batches = log.read(metadata.image().appliedOffset(), config.maxBatchBytes());
            if (batches.isEmpty()) throw new IOException("Committed replay gap");
            for (var batch : batches) {
                if (batch.nextOffset() > committed) break;
                metadata.apply(batch);
            }
        }
        return metadata.image();
    }

    public EpochIndex epochIndex() {
        return log.epochs();
    }

    public SnapshotId baseSnapshot() {
        return baseSnapshot;
    }

    /**
     * Installs {@code id} with no cancellation fence.
     *
     * @see #install(SnapshotId, MetadataImage, java.util.function.BooleanSupplier)
     */
    public void install(SnapshotId id, MetadataImage image) throws IOException {
        install(id, image, () -> true);
    }

    /** Best-effort cleanup after publication and drain; failure leaves recoverable garbage only. */
    public boolean releaseObsoleteGenerations() {
        Path parent = state.root().resolve("generations").toAbsolutePath().normalize();
        try (var children = Files.list(parent)) {
            for (var child : children.toList()) {
                Path target = child.toAbsolutePath().normalize();
                String name = target.getFileName().toString();
                UUID id;
                try {
                    id = UUID.fromString(name);
                } catch (IllegalArgumentException ignored) {
                    continue;
                }
                if (!id.toString().equals(name)
                        || id.equals(generation)
                        || !target.getParent().equals(parent)
                        || !Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) continue;
                try (var paths = Files.walk(target)) {
                    for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                        if (!path.toAbsolutePath().normalize().startsWith(target))
                            throw new IOException("Generation cleanup escaped root");
                        Files.delete(path);
                    }
                }
            }
            files.syncDirectory(parent);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Replaces the active generation with a new empty log that starts at the end of snapshot {@code
     * id}.
     *
     * <p>The new log is prepared and forced first. The GENERATION journal record is the only
     * publication step; until it is written, recovery ignores the prepared directory. {@code
     * allowed} is checked before and after the old log is closed. If it fails after the close, the
     * old log is reopened so the store stays usable.
     *
     * @param image must equal the image stored in snapshot {@code id}
     * @param allowed install fence, for example "this transfer still belongs to the current epoch"
     * @throws InstallCancelled if {@code allowed} returns false before publication
     * @throws IOException if the snapshot does not advance the committed prefix or contradicts the
     *     known committed epoch
     */
    public void install(
            SnapshotId id, MetadataImage image, java.util.function.BooleanSupplier allowed)
            throws IOException {
        if (image.appliedOffset() != id.endOffset()
                || id.endOffset() < committed
                || id.endOffset() <= log.start())
            throw new IOException("Snapshot does not advance safe prefix");
        if (log.epochs().positionAt(committed).lastEpoch() > id.lastEpoch())
            throw new IOException("Snapshot contradicts known committed epoch");
        var snapshots =
                new SnapshotStore(state.root(), state.identity(), files, state, 64 * 1024 * 1024);
        if (!image.equals(snapshots.load(id)))
            throw new IOException("Installed image differs from immutable snapshot");
        UUID next = UUID.randomUUID();
        Path generationDirectory = state.root().resolve("generations").resolve(next.toString());
        Files.createDirectory(generationDirectory);
        files.syncDirectory(generationDirectory.getParent());
        Path directory = generationDirectory.resolve("log");
        QuorumLog replacement = null;
        try {
            replacement =
                    QuorumLog.open(
                            directory,
                            config,
                            id.endOffset(),
                            id.endOffset(),
                            id.lastEpoch(),
                            true,
                            files);
            replacement.flush();
            files.syncDirectory(generationDirectory);
            files.syncDirectory(generationDirectory.getParent());
            if (!allowed.getAsBoolean()) throw new InstallCancelled();
            log.close();
            if (!allowed.getAsBoolean()) {
                log =
                        QuorumLog.open(
                                directory(),
                                config,
                                start,
                                committed,
                                baseSnapshot == null ? start : baseSnapshot.endOffset(),
                                baseSnapshot == null ? 0 : baseSnapshot.lastEpoch(),
                                false,
                                files);
                throw new InstallCancelled();
            }
            // Publication is the journal record: recovery ignores an unreferenced prepared
            // generation.
            state.journal()
                    .append(StateJournal.GENERATION, encodeGeneration(next, id.endOffset(), id));
            generation = next;
            start = id.endOffset();
            committed = id.endOffset();
            baseSnapshot = id;
            log = replacement;
            replacement = null;
            snapshots.publishInstalled(id);
        } finally {
            if (replacement != null) replacement.close();
        }
    }

    private byte[] encodeBoundary(long end) {
        return ByteBuffer.allocate(24)
                .putLong(generation.getMostSignificantBits())
                .putLong(generation.getLeastSignificantBits())
                .putLong(end)
                .array();
    }

    /** Log directory of the active generation. */
    public Path directory() {
        return state.root().resolve("generations").resolve(generation.toString()).resolve("log");
    }

    public QuorumLog log() {
        return log;
    }

    public UUID generation() {
        return generation;
    }

    public long committedOffset() {
        return committed;
    }

    @Override
    public void close() throws IOException {
        if (log != null) log.close();
    }
}

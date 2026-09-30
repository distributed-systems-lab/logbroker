package vn.huyqt.logbroker.controller.log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;
import vn.huyqt.logbroker.storage.*;

/**
 * Metadata payload adapter; storage remains unaware of elections and quorum progress.
 *
 * <p>Stores one {@link QuorumEntry} per ordinary {@link PartitionLog} record (timestamp 0, null
 * key, no headers), one storage batch per leader batch, and keeps an {@link EpochIndex} rebuilt
 * from disk after every mutation. See {@code docs/controller-storage-v1.md}.
 *
 * <p>The physical start is a segment base; the logical origin is the recovery snapshot boundary
 * and may lie after it. Batches before the origin stay on disk but are not readable through
 * {@link #read} and are not part of the epoch index.
 *
 * <p>Appends are not durable until {@link #flush}. Not thread-safe; the caller serializes access.
 * The returned {@link EpochIndex} is immutable and may be handed to other threads.
 */
public final class QuorumLog implements AutoCloseable {
  private final PartitionLog log;
  private final LogConfig config;
  private long originOffset, originEpoch;
  private EpochIndex index;

  private QuorumLog(PartitionLog log, LogConfig config, long originOffset, long originEpoch)
      throws IOException {
    this.log = log;
    this.config = config;
    this.originOffset = originOffset;
    this.originEpoch = originEpoch;
    rebuild();
  }

  /**
   * Opens a log whose logical origin is its physical start, with origin epoch 0.
   *
   * @see #open(Path, LogConfig, long, long, long, long, boolean, DurableFiles)
   */
  public static QuorumLog open(
      Path path, LogConfig config, long start, long minimumEnd, boolean create, DurableFiles files)
      throws IOException {
    return open(path, config, start, minimumEnd, 0, create, files);
  }

  /**
   * Opens a log whose logical origin is its physical start.
   *
   * @see #open(Path, LogConfig, long, long, long, long, boolean, DurableFiles)
   */
  public static QuorumLog open(
      Path path,
      LogConfig config,
      long start,
      long minimumEnd,
      long originEpoch,
      boolean create,
      DurableFiles files)
      throws IOException {
    return open(path, config, start, minimumEnd, start, originEpoch, create, files);
  }

  /**
   * Opens and recovers the log, then replays it to build the epoch index. The underlying log is
   * closed if validation fails.
   *
   * @param physicalStart segment base offset the log directory starts at
   * @param minimumEnd committed floor; recovery fails rather than remove data below it
   * @param logicalStart logical origin; must be the physical start or a retained batch end
   * @param originEpoch epoch of the last entry before {@code logicalStart}
   * @param create whether a missing log directory may be created
   * @throws IOException if recovery fails, a record is not a valid quorum entry, or the origin or
   *     epoch sequence is inconsistent
   */
  public static QuorumLog open(
      Path path,
      LogConfig config,
      long physicalStart,
      long minimumEnd,
      long logicalStart,
      long originEpoch,
      boolean create,
      DurableFiles files)
      throws IOException {
    PartitionLog log =
        PartitionLog.open(
            path,
            config,
            new LogOpenOptions(physicalStart, minimumEnd, create, files::syncDirectory));
    try {
      return new QuorumLog(log, config, logicalStart, originEpoch);
    } catch (IOException | RuntimeException e) {
      log.close();
      throw e;
    }
  }

  /**
   * Appends {@code entries} as one leader batch at the current end. The batch is not durable until
   * {@link #flush}.
   *
   * @return the batch with its assigned base offset
   * @throws IllegalArgumentException if an entry epoch differs from {@code epoch}, the epoch would
   *     regress, or the batch exceeds the configured batch size
   */
  public QuorumBatch append(long epoch, List<QuorumEntry> entries) throws IOException {
    var batch = new QuorumBatch(log.logEndOffset(), entries);
    if (batch.entries().getFirst().epoch() != epoch)
      throw new IllegalArgumentException("Wrong append epoch");
    appendReplica(batch);
    return batch;
  }

  /**
   * Appends {@code batch} unchanged, preserving the leader's offsets and batch boundary. The batch
   * is not durable until {@link #flush}.
   *
   * @throws IllegalArgumentException if the batch does not start at {@link #end()}, its epoch is
   *     lower than the last retained epoch, or it exceeds the configured batch size
   */
  public void appendReplica(QuorumBatch batch) throws IOException {
    if (batch.baseOffset() != end()
        || batch.entries().getFirst().epoch() < index.positionAt(end()).lastEpoch())
      throw new IllegalArgumentException("Replica base or epoch mismatch");
    var records = new ArrayList<LogRecord>();
    // The batch must fit both encodings: the replication wire form (base, count and CRC, plus a
    // length prefix per entry) and the storage batch (30-byte header plus record framing).
    long wireBytes = 16;
    for (var entry : batch.entries()) {
      byte[] payload = QuorumEntryCodec.encode(entry);
      wireBytes = Math.addExact(wireBytes, 4L + payload.length);
      records.add(new LogRecord(0, null, payload, List.of()));
    }
    if (wireBytes > config.maxBatchBytes()
        || 30L + RecordPayloadCodec.encodedSize(records) > config.maxBatchBytes())
      throw new IllegalArgumentException("Metadata batch too large");
    log.append(records);
    rebuild();
  }

  /**
   * Reads whole batches from {@code offset} within {@code budget} bytes, with the budget rules of
   * {@link PartitionLog#read}.
   *
   * @throws IOException if {@code offset} precedes the logical origin or a stored record is not a
   *     valid quorum entry or batch
   */
  public List<QuorumBatch> read(long offset, int budget) throws IOException {
    if (offset < originOffset) throw new IOException("Offset precedes quorum recovery base");
    return readPhysical(offset, budget);
  }

  private List<QuorumBatch> readPhysical(long offset, int budget) throws IOException {
    var result = new ArrayList<QuorumBatch>();
    for (var batch : log.read(offset, budget)) {
      var entries = new ArrayList<QuorumEntry>();
      for (var record : batch.records()) {
        if (record.timestamp() != 0 || record.key() != null || !record.headers().isEmpty())
          throw new IOException("Invalid quorum record");
        entries.add(QuorumEntryCodec.decode(record.value()));
      }
      try {
        result.add(new QuorumBatch(batch.baseOffset(), entries));
      } catch (IllegalArgumentException e) {
        throw new IOException("Invalid quorum batch", e);
      }
    }
    return List.copyOf(result);
  }

  // The index is always derived from what storage returns, never patched in memory. Batches
  // before the logical origin are decoded too, so an invalid record there also fails the rebuild.
  private void rebuild() throws IOException {
    var batches = new ArrayList<QuorumBatch>();
    long offset = log.logStartOffset();
    while (offset < log.logEndOffset()) {
      var next = readPhysical(offset, config.maxBatchBytes());
      if (next.isEmpty()) throw new IOException("Quorum replay gap");
      batches.addAll(next);
      offset = next.getLast().nextOffset();
    }
    if (originOffset < log.logStartOffset()
        || originOffset > log.logEndOffset()
        || originOffset != log.logStartOffset()
            && batches.stream().noneMatch(b -> b.nextOffset() == originOffset))
      throw new IOException("Invalid logical quorum origin");
    try {
      index =
          new EpochIndex(
              originOffset,
              originEpoch,
              batches.stream().filter(b -> b.baseOffset() >= originOffset).toList());
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid quorum epoch sequence", e);
    }
  }

  /**
   * Removes every batch after {@code end}. Both checks run before storage is modified. This call
   * alone is not crash-atomic; {@link vn.huyqt.logbroker.controller.persistence.GenerationStore}
   * journals a truncate intent around it.
   *
   * @param knownCommit committed offset the truncation must not cross
   * @throws IllegalArgumentException if {@code end} is below {@code knownCommit} or is not a
   *     retained batch boundary
   */
  public void truncate(long end, long knownCommit) throws IOException {
    if (end < knownCommit) throw new IllegalArgumentException("Truncation crosses commit");
    index.positionAt(end);
    log.truncateTo(end);
    rebuild();
  }

  /** Forces appended data to storage and returns the exclusive durable end offset. */
  public long flush() throws IOException {
    return log.flush();
  }

  public long end() {
    return log.logEndOffset();
  }

  /** Returns the exclusive end covered by recovery or the last successful {@link #flush}. */
  public long durableEnd() {
    return log.durableEndOffset();
  }

  /** Returns the logical origin, which may be after the physical start. */
  public long start() {
    return originOffset;
  }

  /**
   * Moves the logical origin forward to a retained boundary. Only the in-memory origin changes;
   * segment deletion and its durable intent are the caller's responsibility.
   *
   * @param epoch epoch expected at {@code start}; it must match the retained boundary
   * @throws IllegalArgumentException if {@code start} moves backwards, is not a retained boundary,
   *     or its epoch differs from {@code epoch}
   */
  public void advanceStart(long start, long epoch) throws IOException {
    if (start < originOffset || index.positionAt(start).lastEpoch() != epoch)
      throw new IllegalArgumentException("Invalid recovery base");
    originOffset = start;
    originEpoch = epoch;
    rebuild();
  }

  /** Rebuilds the epoch index after the caller changed {@link #storage()} directly. */
  public void refresh() throws IOException {
    rebuild();
  }

  /** Returns the immutable index as of the last mutation. */
  public EpochIndex epochs() {
    return index;
  }

  /**
   * Returns the underlying storage log. Callers that mutate it directly must call {@link
   * #refresh()} afterwards.
   */
  public PartitionLog storage() {
    return log;
  }

  @Override
  public void close() throws IOException {
    log.close();
  }
}

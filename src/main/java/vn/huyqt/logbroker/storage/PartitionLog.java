package vn.huyqt.logbroker.storage;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A single-writer partition log backed by ordered data segments and rebuildable indexes.
 *
 * <p>An instance holds an exclusive lock on {@code <directory>/.lock} from {@link #open} until
 * {@link #close()}. File format, durability contract and recovery rules are specified in {@code
 * docs/storage-format-v1.md}.
 *
 * <p>Offsets: {@link #logStartOffset()} is the base of the first retained segment, {@link
 * #logEndOffset()} is the exclusive end visible to readers, and {@link #durableEndOffset()} is
 * the exclusive end covered by the last force. Only records below the durable end are expected to
 * survive a power loss, and the durable end is not a replication high watermark.
 *
 * <p>Lifecycle: an I/O error after a mutation has started moves the log to a failed state in
 * which every call except {@link #close()} throws {@link IllegalStateException} carrying the
 * first failure; the caller must reopen to recover. Validation errors reported before any file
 * is changed leave the log usable.
 *
 * <p>Thread-safe. Mutations take an exclusive lock; reads and offset queries share a read lock
 * and may run concurrently with each other.
 */
public final class PartitionLog implements AutoCloseable {
  private enum State {
    OPEN,
    FAILED,
    CLOSED
  }

  private final Path directory;
  private final LogConfig config;
  private final LogIo io;
  private final LogOpenOptions options;
  private final FileChannel lockChannel;
  private final FileLock fileLock;
  private final TreeMap<Long, LogSegment> segments = new TreeMap<>();
  private final TreeMap<Long, OffsetIndex> indexes = new TreeMap<>();
  private final ReentrantReadWriteLock guard = new ReentrantReadWriteLock();
  private State state = State.OPEN;
  private IOException firstFailure;
  private long logEndOffset;
  private long durableEndOffset;

  private PartitionLog(
      Path directory,
      LogConfig config,
      LogIo io,
      LogOpenOptions options,
      FileChannel lockChannel,
      FileLock fileLock) {
    this.directory = directory;
    this.config = config;
    this.io = io;
    this.options = options;
    this.lockChannel = lockChannel;
    this.fileLock = fileLock;
  }

  /**
   * Opens and recovers a partition while holding its directory lock until close.
   *
   * <p>Uses the standalone behavior: the directory is created if needed, the log starts at offset
   * 0, and directory entries are not synced. After recovery the durable end equals the recovered
   * end.
   *
   * @throws IOException if the directory is already locked, by this or another process
   * @throws CorruptLogException if recovery finds data it may not repair
   */
  public static PartitionLog open(Path directory, LogConfig config) throws IOException {
    return open(directory, config, new LogIo());
  }

  static PartitionLog open(Path directory, LogConfig config, LogIo io) throws IOException {
    return open(directory, config, io, LogOpenOptions.standalone());
  }

  /**
   * Opens and recovers a log whose origin, committed floor and directory sync are chosen by the
   * caller; see {@link LogOpenOptions}. An empty log gets its first segment at {@code
   * options.startOffset()}.
   *
   * @throws IOException if {@code options.createIfMissing()} is false and the directory or its
   *     data segments are missing, or if the directory is already locked
   * @throws CorruptLogException if recovery finds data it may not repair or would end below
   *     {@code options.minimumEndOffset()}
   */
  public static PartitionLog open(Path directory, LogConfig config, LogOpenOptions options)
      throws IOException {
    return open(directory, config, new LogIo(), options);
  }

  static PartitionLog open(Path directory, LogConfig config, LogIo io, LogOpenOptions options)
      throws IOException {
    Objects.requireNonNull(directory, "directory");
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(io, "io");
    Objects.requireNonNull(options, "options");
    if (!options.createIfMissing()) {
      if (!Files.isDirectory(directory))
        throw new IOException("Published log directory is missing");
      try (var paths = Files.list(directory)) {
        if (paths.noneMatch(path -> path.getFileName().toString().endsWith(".log")))
          throw new IOException("Published log has no data segments");
      }
    }
    Files.createDirectories(directory);
    FileChannel lockChannel = FileChannel.open(directory.resolve(".lock"), CREATE, WRITE);
    FileLock fileLock = null;
    PartitionLog log = null;
    try {
      try {
        fileLock = lockChannel.tryLock();
      } catch (OverlappingFileLockException e) {
        throw new IOException("Partition directory is already locked: " + directory, e);
      }
      if (fileLock == null)
        throw new IOException("Partition directory is already locked: " + directory);
      log = new PartitionLog(directory, config, io, options, lockChannel, fileLock);
      LogRecovery.Result recovered = LogRecovery.recover(directory, config, io, options);
      for (LogRecovery.SegmentInfo info : recovered.segments()) {
        LogSegment segment =
            LogSegment.open(
                LogSegment.dataPath(directory, info.baseOffset()), info.baseOffset(), io);
        log.segments.put(info.baseOffset(), segment);
        log.indexes.put(info.baseOffset(), info.index());
      }
      if (log.segments.isEmpty()) {
        long base = options.startOffset();
        LogSegment segment = LogSegment.open(LogSegment.dataPath(directory, base), base, io);
        log.segments.put(base, segment);
        OffsetIndex index = new OffsetIndex(config.indexIntervalBytes());
        log.indexes.put(base, index);
        index.write(OffsetIndex.indexPath(directory, base), io);
        segment.force();
      }
      options.directories().sync(directory);
      log.logEndOffset = recovered.nextOffset();
      log.durableEndOffset = recovered.nextOffset();
      return log;
    } catch (IOException | RuntimeException error) {
      if (log != null) {
        for (LogSegment segment : log.segments.values()) {
          try {
            segment.close();
          } catch (IOException e) {
            error.addSuppressed(e);
          }
        }
      }
      if (fileLock != null) {
        try {
          fileLock.release();
        } catch (IOException e) {
          error.addSuppressed(e);
        }
      }
      try {
        lockChannel.close();
      } catch (IOException e) {
        error.addSuppressed(e);
      }
      throw error;
    }
  }

  /**
   * Appends one batch; call {@link #flush()} to make the returned offsets durable.
   *
   * <p>The batch is encoded and validated before any file is touched. Success means the write
   * loop completed; the bytes are immediately visible to {@link #read}. If the batch does not fit
   * in the active segment, that segment is forced first, which may advance the durable end to the
   * previous log end.
   *
   * @return offsets assigned to the batch, starting at the previous log end
   * @throws IllegalArgumentException if {@code records} is empty or cannot be encoded within
   *     {@link LogConfig#maxBatchBytes()}; the log stays open
   * @throws IOException if a write or force fails; the log becomes failed and the batch may still
   *     reappear after recovery
   */
  public AppendResult append(List<LogRecord> records) throws IOException {
    guard.writeLock().lock();
    try {
      ensureOpen();
      byte[] bytes = BatchCodec.encode(logEndOffset, records, config.maxBatchBytes());
      long oldEnd = logEndOffset;
      try {
        LogSegment active = segments.lastEntry().getValue();
        if (active.size() + bytes.length > config.segmentBytes()) {
          // The sealed segment must be durable before a new active segment is published.
          active.force();
          options.directories().sync(directory);
          durableEndOffset = oldEnd;
          active = LogSegment.open(LogSegment.dataPath(directory, oldEnd), oldEnd, io);
          segments.put(oldEnd, active);
          indexes.put(oldEnd, new OffsetIndex(config.indexIntervalBytes()));
        }
        long position = active.append(bytes);
        OffsetIndex index = indexes.get(active.baseOffset());
        index.consider(oldEnd, position);
        index.write(OffsetIndex.indexPath(directory, active.baseOffset()), io);
        logEndOffset = Math.addExact(oldEnd, records.size());
        return new AppendResult(oldEnd, logEndOffset);
      } catch (IOException e) {
        markFailed(e);
        throw e;
      }
    } finally {
      guard.writeLock().unlock();
    }
  }

  /**
   * Reads whole batches from an offset; the first batch may exceed the byte budget.
   *
   * <p>Reads up to the log end, including data not yet flushed. The first batch returned is the
   * one containing {@code offset}, so it may start before it; callers filter earlier records.
   * Later batches are added only while the total encoded size stays within {@code maxBytes}, so
   * the total never exceeds {@code max(maxBytes, maxBatchBytes)}.
   *
   * @param offset offset in {@code [logStartOffset, logEndOffset]}; reading at the end returns an
   *     empty list
   * @param maxBytes positive budget in storage-encoded bytes
   * @throws IllegalArgumentException if {@code offset} is outside the log or {@code maxBytes} is
   *     not positive
   * @throws CorruptLogException if a stored batch fails validation
   */
  public List<RecordBatch> read(long offset, int maxBytes) throws IOException {
    guard.readLock().lock();
    try {
      ensureOpen();
      if (offset < segments.firstKey() || offset > logEndOffset || maxBytes <= 0) {
        throw new IllegalArgumentException("Invalid read range or budget");
      }
      if (offset == logEndOffset) return List.of();
      List<RecordBatch> result = new ArrayList<>();
      long accumulated = 0;
      for (Map.Entry<Long, LogSegment> item :
          segments.tailMap(segments.floorKey(offset), true).entrySet()) {
        LogSegment segment = item.getValue();
        OffsetIndex.Entry floor = indexes.get(item.getKey()).floor(offset);
        long position = floor == null ? 0 : floor.position();
        long size = segment.size();
        while (position < size) {
          RecordBatch batch = readBatch(segment, position, size);
          position += batch.encodedSize();
          if (batch.nextOffset() <= offset) continue;
          if (!result.isEmpty() && accumulated + batch.encodedSize() > maxBytes) {
            return List.copyOf(result);
          }
          result.add(batch);
          accumulated += batch.encodedSize();
          if (accumulated >= maxBytes) return List.copyOf(result);
        }
      }
      return List.copyOf(result);
    } finally {
      guard.readLock().unlock();
    }
  }

  private RecordBatch readBatch(LogSegment segment, long position, long size) throws IOException {
    try {
      if (size - position < BatchCodec.HEADER_BYTES) {
        throw new CorruptLogException(
            segment.path() + " at byte " + position + ": incomplete header");
      }
      byte[] prefix = segment.readBytes(position, BatchCodec.HEADER_BYTES);
      BatchCodec.validateHeaderPrefix(prefix, config.maxBatchBytes());
      int length = ByteBuffer.wrap(prefix).getInt(6);
      if (length > size - position) {
        throw new CorruptLogException(
            segment.path() + " at byte " + position + ": incomplete batch");
      }
      return BatchCodec.decode(segment.readBytes(position, length), config.maxBatchBytes());
    } catch (CorruptLogException e) {
      throw new CorruptLogException(
          segment.path() + " at byte " + position + ": " + e.getMessage(), e);
    }
  }

  /**
   * Truncates at a batch boundary and rebuilds the affected sparse index.
   *
   * <p>Later segments are deleted from the end, the boundary segment is cut, and all segments are
   * forced and the directory synced before the new end is published as both log end and durable
   * end. The operation is not atomic across a crash: reopen and retry the same offset. The
   * committed floor in {@link LogOpenOptions} is not checked here.
   *
   * @param offset batch base offset or the log end (a no-op)
   * @throws IllegalArgumentException if {@code offset} is outside the log or inside a batch; no
   *     file has been changed
   * @throws IOException if a file operation fails; the log becomes failed
   */
  public void truncateTo(long offset) throws IOException {
    guard.writeLock().lock();
    try {
      ensureOpen();
      if (offset < segments.firstKey() || offset > logEndOffset) {
        throw new IllegalArgumentException("Offset outside log");
      }
      if (offset == logEndOffset) return;
      Map.Entry<Long, LogSegment> targetEntry = segments.floorEntry(offset);
      if (targetEntry == null) throw new IllegalArgumentException("Offset outside log");
      LogSegment target = targetEntry.getValue();
      long targetPosition = 0;
      if (offset != target.baseOffset()) {
        long size = target.size();
        OffsetIndex.Entry floor = indexes.get(target.baseOffset()).floor(offset);
        long position = floor == null ? 0 : floor.position();
        boolean found = false;
        while (position < size) {
          RecordBatch batch = readBatch(target, position, size);
          if (batch.baseOffset() == offset) {
            targetPosition = position;
            found = true;
            break;
          }
          if (batch.nextOffset() > offset) break;
          position += batch.encodedSize();
        }
        if (!found) throw new IllegalArgumentException("Offset is not a batch boundary");
      }

      try {
        // Remove later segments from the end so a failed operation leaves a recoverable
        // prefix.
        for (Long base : new ArrayList<>(segments.descendingKeySet())) {
          if (base <= target.baseOffset()) break;
          LogSegment segment = segments.get(base);
          segment.close();
          io.delete(segment.path());
          Path indexPath = OffsetIndex.indexPath(directory, base);
          if (Files.exists(indexPath)) io.delete(indexPath);
          segments.remove(base);
          indexes.remove(base);
        }
        target.truncate(targetPosition);
        OffsetIndex rebuilt = new OffsetIndex(config.indexIntervalBytes());
        long position = 0;
        while (position < targetPosition) {
          RecordBatch batch = readBatch(target, position, targetPosition);
          rebuilt.consider(batch.baseOffset(), position);
          position += batch.encodedSize();
        }
        rebuilt.write(OffsetIndex.indexPath(directory, target.baseOffset()), io);
        indexes.put(target.baseOffset(), rebuilt);
        for (LogSegment segment : segments.values()) segment.force();
        options.directories().sync(directory);
        logEndOffset = offset;
        durableEndOffset = offset;
      } catch (IOException e) {
        markFailed(e);
        throw e;
      }
    } finally {
      guard.writeLock().unlock();
    }
  }

  /**
   * Forces data to storage and returns the exclusive durable end offset.
   *
   * <p>Every segment is forced and then the directory synced; only after both succeed does the
   * durable end move to the current log end.
   *
   * @throws IOException if a force or sync fails; the log becomes failed and the durable end is
   *     unchanged
   */
  public long flush() throws IOException {
    guard.writeLock().lock();
    try {
      ensureOpen();
      try {
        return forceAll();
      } catch (IOException e) {
        markFailed(e);
        throw e;
      }
    } finally {
      guard.writeLock().unlock();
    }
  }

  private long forceAll() throws IOException {
    for (LogSegment segment : segments.values()) segment.force();
    options.directories().sync(directory);
    durableEndOffset = logEndOffset;
    return durableEndOffset;
  }

  /** Returns the first offset covered by retained segments. */
  public long logStartOffset() {
    guard.readLock().lock();
    try {
      ensureOpen();
      return segments.firstKey();
    } finally {
      guard.readLock().unlock();
    }
  }

  /** Returns the exclusive end offset visible to readers. */
  public long logEndOffset() {
    guard.readLock().lock();
    try {
      ensureOpen();
      return logEndOffset;
    } finally {
      guard.readLock().unlock();
    }
  }

  /** Returns the exclusive end offset established by recovery or a successful force. */
  public long durableEndOffset() {
    guard.readLock().lock();
    try {
      ensureOpen();
      return durableEndOffset;
    } finally {
      guard.readLock().unlock();
    }
  }

  // After a mutating I/O failure, callers must reopen to recover the on-disk
  /**
   * Computes the first retained segment without mutating storage.
   *
   * <p>Returns the base offset of the last segment whose base is at most {@code boundary}, i.e.
   * the segment containing {@code boundary}; the active segment's base when {@code boundary} lies
   * beyond it; or the current start when {@code boundary} lies before it. This is the log start
   * that {@link #deleteSegmentsBefore(long)} would produce.
   */
  public long prefixStartAfter(long boundary) {
    guard.readLock().lock();
    try {
      ensureOpen();
      if (boundary < 0) throw new IllegalArgumentException("Negative boundary");
      long retained = segments.firstKey();
      for (Long base : segments.keySet()) {
        Long next = segments.higherKey(base);
        if (next == null || next > boundary) break;
        retained = next;
      }
      return retained;
    } finally {
      guard.readLock().unlock();
    }
  }

  /**
   * Caller owns durable crash-recovery intent; active segment is never deleted.
   *
   * <p>Deletes whole segments below {@link #prefixStartAfter(long)} together with their indexes,
   * then syncs the directory. The log does not persist its start offset: before calling, the
   * caller must durably record the new start so that it can finish an interrupted deletion and
   * reopen with a matching {@link LogOpenOptions#startOffset()}. Log end and durable end are
   * unchanged.
   *
   * @return the new {@link #logStartOffset()}
   * @throws IOException if a delete or sync fails; the log becomes failed
   */
  public long deleteSegmentsBefore(long boundary) throws IOException {
    guard.writeLock().lock();
    try {
      ensureOpen();
      long retained = prefixStartAfter(boundary);
      try {
        for (Long base : new ArrayList<>(segments.headMap(retained, false).keySet())) {
          var segment = segments.get(base);
          segment.close();
          io.delete(segment.path());
          Path index = OffsetIndex.indexPath(directory, base);
          if (Files.exists(index)) io.delete(index);
          segments.remove(base);
          indexes.remove(base);
        }
        options.directories().sync(directory);
        return segments.firstKey();
      } catch (IOException e) {
        markFailed(e);
        throw e;
      }
    } finally {
      guard.writeLock().unlock();
    }
  }

  // After a mutating I/O failure, callers must reopen to recover the on-disk
  // state.
  private void markFailed(IOException error) {
    if (firstFailure == null) firstFailure = error;
    state = State.FAILED;
  }

  private void ensureOpen() {
    if (state != State.OPEN) throw new IllegalStateException("Log is " + state, firstFailure);
  }

  /**
   * Flushes an open log, then releases its segments and directory lock.
   *
   * <p>A failed log is only cleaned up, without a flush attempt. Resources are released even if
   * the flush fails, and repeated calls are no-ops.
   *
   * @throws IOException the flush failure, or the first close failure, with later ones suppressed
   */
  @Override
  public void close() throws IOException {
    guard.writeLock().lock();
    try {
      if (state == State.CLOSED) return;
      IOException failure = null;
      if (state == State.OPEN) {
        try {
          forceAll();
        } catch (IOException e) {
          markFailed(e);
          failure = e;
        }
      }
      for (LogSegment segment : segments.values()) {
        try {
          segment.close();
        } catch (IOException e) {
          failure = combine(failure, e);
        }
      }
      try {
        fileLock.release();
      } catch (IOException e) {
        failure = combine(failure, e);
      }
      try {
        lockChannel.close();
      } catch (IOException e) {
        failure = combine(failure, e);
      }
      state = State.CLOSED;
      if (failure != null) throw failure;
    } finally {
      guard.writeLock().unlock();
    }
  }

  private static IOException combine(IOException first, IOException later) {
    if (first == null) return later;
    first.addSuppressed(later);
    return first;
  }
}

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

/** A single-writer partition log backed by ordered data segments and rebuildable indexes. */
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

  /** Opens and recovers a partition while holding its directory lock until close. */
  public static PartitionLog open(Path directory, LogConfig config) throws IOException {
    return open(directory, config, new LogIo());
  }

  static PartitionLog open(Path directory, LogConfig config, LogIo io) throws IOException {
    return open(directory, config, io, LogOpenOptions.standalone());
  }

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

  /** Appends one batch; call {@link #flush()} to make the returned offsets durable. */
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

  /** Reads whole batches from an offset; the first batch may exceed the byte budget. */
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

  /** Truncates at a batch boundary and rebuilds the affected sparse index. */
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

  /** Forces data to storage and returns the exclusive durable end offset. */
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
  /** Computes the first retained segment without mutating storage. */
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

  /** Caller owns durable crash-recovery intent; active segment is never deleted. */
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

  /** Flushes an open log, then releases its segments and directory lock. */
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

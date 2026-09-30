package vn.huyqt.logbroker.storage;

import java.util.Objects;

/**
 * Caller-supplied origin and committed floor; checked before recovery repairs files.
 *
 * <p>Used by the controller metadata log; the standalone broker open keeps the Phase 1/2
 * defaults. See {@code docs/storage-format-v1.md}.
 *
 * @param startOffset base offset the first data segment must have; also the base of the initial
 *     segment when an empty log is created
 * @param minimumEndOffset committed floor; open fails if the recovered end offset is below it,
 *     before any torn tail is cut
 * @param createIfMissing if {@code false}, the directory must already exist and contain at least
 *     one data segment
 * @param directories directory sync used whenever storage publishes durable state
 */
public record LogOpenOptions(
    long startOffset,
    long minimumEndOffset,
    boolean createIfMissing,
    DirectoryDurability directories) {
  public LogOpenOptions {
    Objects.requireNonNull(directories);
    if (startOffset < 0 || minimumEndOffset < startOffset)
      throw new IllegalArgumentException("Invalid recovery bounds");
  }

  /** Phase 1 behavior: start at offset 0, no floor, create if missing, no directory sync. */
  static LogOpenOptions standalone() {
    return new LogOpenOptions(0, 0, true, path -> {});
  }
}

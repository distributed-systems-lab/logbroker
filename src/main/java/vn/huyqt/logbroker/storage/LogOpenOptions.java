package vn.huyqt.logbroker.storage;

import java.util.Objects;

/** Caller-supplied origin and committed floor; checked before recovery repairs files. */
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

  static LogOpenOptions standalone() {
    return new LogOpenOptions(0, 0, true, path -> {});
  }
}

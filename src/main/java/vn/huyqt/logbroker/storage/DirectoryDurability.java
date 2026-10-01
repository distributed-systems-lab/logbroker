package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Makes directory entry changes (created, truncated or deleted segment files) durable.
 *
 * <p>Java has no portable directory fsync, so the implementation is injected through {@link
 * LogOpenOptions}. The standalone open path uses a no-op, which keeps the Phase 1 behavior and
 * its directory-entry caveat described in {@code docs/storage-format-v1.md}.
 */
@FunctionalInterface
public interface DirectoryDurability {
  /**
   * Syncs {@code directory}. Storage calls this after forcing data files and before publishing a
   * new durable end offset, after deleting segments, and at the end of open and intent recovery.
   *
   * @throws IOException if the directory could not be synced; the caller treats this as a failed
   *     mutation
   */
  void sync(Path directory) throws IOException;
}

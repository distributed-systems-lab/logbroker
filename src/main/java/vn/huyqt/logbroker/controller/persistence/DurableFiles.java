package vn.huyqt.logbroker.controller.persistence;

import static java.nio.file.StandardOpenOption.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/**
 * Strict filesystem publication boundary. Unsupported providers fail rather than weaken durability.
 *
 * <p>A file is only considered published once both its contents and the directory entry naming it
 * have been forced. This class never falls back to file-only force or rename-only durability; see
 * {@code docs/controller-storage-v1.md}. Methods are non-final so tests can inject faults.
 */
public class DurableFiles {
  /**
   * Forces the contents and metadata of an existing file to stable storage.
   *
   * <p>This does not make the file's name durable; callers publishing a new name must also call
   * {@link #syncDirectory} on its parent.
   */
  public void forceFile(Path path) throws IOException {
    try (var channel = FileChannel.open(path, READ, WRITE)) {
      channel.force(true);
    }
  }

  /**
   * Forces a directory so that entries created, renamed or deleted inside it survive power loss.
   *
   * @throws IOException if the provider cannot open or force directories (for example native
   *     Windows); the failure is surfaced instead of silently skipping directory durability
   */
  public void syncDirectory(Path path) throws IOException {
    try (var channel = FileChannel.open(path, READ)) {
      channel.force(true);
    } catch (IOException | UnsupportedOperationException e) {
      throw new IOException("Directory durability unsupported on " + path.toAbsolutePath(), e);
    }
  }

  /**
   * Creates {@code path} exclusively, writes {@code bytes} and forces the file contents.
   *
   * <p>The parent directory is not synced; callers must call {@link #syncDirectory} before treating
   * the new name as published.
   *
   * @throws FileAlreadyExistsException if {@code path} already exists
   */
  public void writeNew(Path path, byte[] bytes) throws IOException {
    try (var channel = FileChannel.open(path, CREATE_NEW, WRITE)) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      // Guard against a provider that reports zero progress forever instead of failing.
      while (buffer.hasRemaining())
        if (channel.write(buffer) <= 0) throw new IOException("No write progress");
    }
    forceFile(path);
  }

  /**
   * Fails fast when {@code directory} cannot provide the durability guarantees this class relies
   * on. Called before formatting or opening controller storage.
   */
  public void verifySupport(Path directory) throws IOException {
    if (!Files.isDirectory(directory)) throw new IOException("Missing durability probe directory");
    syncDirectory(directory);
  }
}

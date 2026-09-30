package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionNonzeroBaseTest {
  @TempDir Path root;
  private final LogConfig config = new LogConfig(256, 128, 32);

  private List<LogRecord> records() {
    return List.of(new LogRecord(0, null, new byte[] {1}, List.of()));
  }

  @Test
  void opensAtNonzeroOriginAndPreservesOffsetAcrossRestart() throws Exception {
    var options = new LogOpenOptions(100, 100, true, path -> {});
    try (var log = PartitionLog.open(root, config, options)) {
      assertEquals(100, log.logStartOffset());
      assertEquals(100, log.logEndOffset());
      assertEquals(100, log.append(records()).firstOffset());
      assertThrows(IllegalArgumentException.class, () -> log.read(99, 1));
      assertThrows(IllegalArgumentException.class, () -> log.truncateTo(99));
    }
    try (var log =
        PartitionLog.open(root, config, new LogOpenOptions(100, 101, false, path -> {}))) {
      assertEquals(101, log.logEndOffset());
      assertEquals(100, log.read(100, 1).getFirst().baseOffset());
    }
  }

  @Test
  void missingPublishedLogIsNotCreated() {
    Path missing = root.resolve("missing");
    assertThrows(
        IOException.class,
        () -> PartitionLog.open(missing, config, new LogOpenOptions(100, 100, false, path -> {})));
    assertFalse(Files.exists(missing));
  }

  @Test
  void repairCannotRemoveCheckpointedBatch() throws Exception {
    try (var log = PartitionLog.open(root, config)) {
      log.append(records());
      log.append(records());
    }
    Path data = root.resolve("00000000000000000000.log");
    byte[] full = Files.readAllBytes(data);
    Files.write(data, java.util.Arrays.copyOf(full, full.length - 1));
    byte[] damaged = Files.readAllBytes(data);
    assertThrows(
        CorruptLogException.class,
        () -> PartitionLog.open(root, config, new LogOpenOptions(0, 2, false, path -> {})));
    assertArrayEquals(damaged, Files.readAllBytes(data));
  }
}

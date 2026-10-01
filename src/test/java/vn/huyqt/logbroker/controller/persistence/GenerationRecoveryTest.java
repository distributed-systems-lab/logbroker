package vn.huyqt.logbroker.controller.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.log.QuorumEntry;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.storage.LogConfig;

class GenerationRecoveryTest {
  @TempDir Path root;

  ClusterIdentity identity() {
    return new ClusterIdentity(
        new UUID(0, 1),
        0,
        List.of(
            new ClusterIdentity.Voter(0, "localhost", 19090),
            new ClusterIdentity.Voter(1, "localhost", 19091),
            new ClusterIdentity.Voter(2, "localhost", 19092)));
  }

  @Test
  void checkpointRestoresCommitWithoutTreatingEntireSuffixAsCommitted() throws Exception {
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
      generation.log().flush();
      generation.checkpointCommit(1);
      generation.log().append(2, List.of(new QuorumEntry.ReadBarrier(2)));
      generation.log().flush();
    }
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      assertEquals(1, generation.committedOffset());
      assertEquals(2, generation.log().end());
      assertEquals(1, generation.recoveredImage().appliedOffset());
    }
  }

  @Test
  void unfinishedTruncateIntentResumesAndCannotCrossCommit() throws Exception {
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
      generation.log().append(2, List.of(new QuorumEntry.ReadBarrier(2)));
      generation.log().flush();
      generation.checkpointCommit(1);
      UUID id = generation.generation();
      state
          .journal()
          .append(
              StateJournal.TRUNCATE_INTENT,
              ByteBuffer.allocate(24)
                  .putLong(id.getMostSignificantBits())
                  .putLong(id.getLeastSignificantBits())
                  .putLong(1)
                  .array());
    }
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      assertEquals(1, generation.log().end());
      assertThrows(IllegalArgumentException.class, () -> generation.truncate(0));
    }
  }

  @Test
  void checkpointBeyondDurableDataIsRejected() throws Exception {
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      assertThrows(IllegalArgumentException.class, () -> generation.checkpointCommit(1));
      UUID id = generation.generation();
      state
          .journal()
          .append(
              StateJournal.COMMIT,
              ByteBuffer.allocate(24)
                  .putLong(id.getMostSignificantBits())
                  .putLong(id.getLeastSignificantBits())
                  .putLong(9)
                  .array());
    }
    try (var state = QuorumStateStore.open(root, identity(), io)) {
      assertThrows(IOException.class, () -> GenerationStore.open(state, io, LogConfig.defaults()));
    }
  }

  @Test
  void durableIntentDiscardsDamagedUncommittedSuffixBeforeStrictRecovery() throws Exception {
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    Path logDirectory;
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
      generation.log().append(2, List.of(new QuorumEntry.ReadBarrier(2)));
      generation.log().flush();
      generation.checkpointCommit(1);
      logDirectory = generation.directory();
      UUID id = generation.generation();
      state
          .journal()
          .append(
              StateJournal.TRUNCATE_INTENT,
              ByteBuffer.allocate(24)
                  .putLong(id.getMostSignificantBits())
                  .putLong(id.getLeastSignificantBits())
                  .putLong(1)
                  .array());
    }
    Path data = logDirectory.resolve("00000000000000000000.log");
    byte[] bytes = java.nio.file.Files.readAllBytes(data);
    bytes[bytes.length - 1] ^= 1;
    java.nio.file.Files.write(data, bytes);
    try (var state = QuorumStateStore.open(root, identity(), io);
        var generation = GenerationStore.open(state, io, LogConfig.defaults())) {
      assertEquals(1, generation.log().end());
      assertEquals(1, generation.committedOffset());
    }
  }
}

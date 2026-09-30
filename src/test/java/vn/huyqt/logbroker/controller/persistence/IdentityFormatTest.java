package vn.huyqt.logbroker.controller.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.support.FaultFiles;

class IdentityFormatTest {
  @TempDir Path temporary;

  private ClusterIdentity identity() {
    return new ClusterIdentity(
        new UUID(0, 1),
        0,
        List.of(
            new ClusterIdentity.Voter(0, "localhost", 19090),
            new ClusterIdentity.Voter(1, "localhost", 19091),
            new ClusterIdentity.Voter(2, "localhost", 19092)));
  }

  @Test
  void durableVoteCannotBeChangedInSameEpochEvenAfterRestart() throws Exception {
    Path root = temporary.resolve("node");
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    try (var store = QuorumStateStore.open(root, identity(), io)) {
      store.persistVote(7, 1);
      assertThrows(IllegalStateException.class, () -> store.persistVote(7, 2));
      assertThrows(IllegalStateException.class, () -> store.persistVote(7, -1));
    }
    try (var store = QuorumStateStore.open(root, identity(), io)) {
      assertEquals(7, store.epoch());
      assertEquals(1, store.votedFor());
      assertThrows(IllegalStateException.class, () -> store.persistVote(6, 1));
      store.persistVote(8, -1);
      store.persistVote(8, 2);
    }
  }

  @Test
  void formatDoesNotOverwriteAndStartupDoesNotCreateMissingStorage() throws Exception {
    Path root = temporary.resolve("node");
    var io = new FaultFiles();
    assertThrows(IOException.class, () -> QuorumStateStore.open(root, identity(), io));
    assertFalse(Files.exists(root));
    QuorumStateStore.format(root, identity(), io);
    byte[] original = Files.readAllBytes(root.resolve("identity.bin"));
    assertThrows(IOException.class, () -> QuorumStateStore.format(root, identity(), io));
    assertArrayEquals(original, Files.readAllBytes(root.resolve("identity.bin")));
  }

  @Test
  void wrongClusterAndConcurrentRootLockAreRejected() throws Exception {
    Path root = temporary.resolve("node");
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    try (var store = QuorumStateStore.open(root, identity(), io)) {
      assertThrows(IOException.class, () -> QuorumStateStore.open(root, identity(), io));
    }
    var wrong = new ClusterIdentity(new UUID(0, 2), 0, identity().voters());
    assertThrows(IOException.class, () -> QuorumStateStore.open(root, wrong, io));
  }

  @Test
  void forceFailureNeverPublishesNewVote() throws Exception {
    Path root = temporary.resolve("node");
    var io = new FaultFiles();
    QuorumStateStore.format(root, identity(), io);
    try (var store = QuorumStateStore.open(root, identity(), io)) {
      io.failAfter(1);
      assertThrows(IOException.class, () -> store.persistVote(2, 1));
      assertEquals(0, store.epoch());
      assertEquals(-1, store.votedFor());
    }
  }
}

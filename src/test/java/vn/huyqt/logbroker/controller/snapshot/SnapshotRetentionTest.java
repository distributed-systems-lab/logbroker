package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.support.*;
import vn.huyqt.logbroker.storage.LogConfig;

class SnapshotRetentionTest {
  @TempDir Path root;

  @Test
  void olderSnapshotAndRetainedSuffixRecoverLatestCommittedImage() throws Exception {
    var files = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    var config = new LogConfig(128, 128, 16);
    QuorumStateStore.format(root, identity, files);
    SnapshotId older;
    try (var state = QuorumStateStore.open(root, identity, files);
        var generation = GenerationStore.open(state, files, config)) {
      var snapshots = new SnapshotStore(root, identity, files, state, 65536);
      var metadata = new MetadataStateMachine();
      for (int i = 0; i < 12; i++) {
        var batch = generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
        metadata.apply(batch);
        generation.log().flush();
        generation.checkpointCommit(i + 1);
        if (i == 3 || i == 7 || i == 11) snapshots.create(metadata.image(), 1);
      }
      older = snapshots.retained().get(1);
      generation.retainPrefix(older.endOffset());
      snapshots.releaseObsolete();
      assertEquals(older.endOffset(), generation.log().start());
      assertTrue(generation.log().storage().logStartOffset() > 0);
      assertEquals(
          2,
          Files.list(root.resolve("snapshots"))
              .filter(p -> p.toString().endsWith(".snapshot"))
              .count());
    }
    try (var state = QuorumStateStore.open(root, identity, files);
        var generation = GenerationStore.open(state, files, config)) {
      assertEquals(12, generation.recoveredImage().appliedOffset());
      assertEquals(older.endOffset(), generation.log().start());
    }
  }

  @Test
  void pinnedOrGenerationReferencedSnapshotCannotBeUnlinked() throws Exception {
    var files = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, files);
    try (var state = QuorumStateStore.open(root, identity, files)) {
      var snapshots = new SnapshotStore(root, identity, files, state, 65536);
      var first = snapshots.create(new MetadataImage(1, List.of()), 1);
      try (var pin = snapshots.pin(first)) {
        snapshots.create(new MetadataImage(2, List.of()), 1);
        snapshots.create(new MetadataImage(3, List.of()), 1);
        snapshots.releaseObsolete();
        assertTrue(Files.exists(snapshots.path(first)));
        assertTrue(pin.length() > 0);
      }
      snapshots.releaseObsolete();
      assertFalse(Files.exists(snapshots.path(first)));
    }
  }

  @Test
  void generationReferenceSurvivesRotationUntilRecoveryBaseIsRepublished() throws Exception {
    var files = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, files);
    try (var state = QuorumStateStore.open(root, identity, files);
        var generation = GenerationStore.open(state, files, LogConfig.defaults())) {
      var snapshots = new SnapshotStore(root, identity, files, state, 65536);
      SnapshotId first = null;
      for (int i = 1; i <= 4; i++) {
        generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
        generation.log().flush();
        generation.checkpointCommit(i);
        var id = snapshots.create(new MetadataImage(i, List.of()), 1);
        if (i == 1) first = id;
        if (i == 2) generation.retainPrefix(1);
      }
      snapshots.releaseObsolete();
      assertTrue(Files.exists(snapshots.path(first)));
      generation.retainPrefix(3);
      snapshots.releaseObsolete();
      assertFalse(Files.exists(snapshots.path(first)));
    }
  }

  @Test
  void unfinishedRetentionRecoversAfterEachDurabilityBoundary() throws Exception {
    for (int failure = 1; failure <= 4; failure++) {
      Path directory = root.resolve("failure-" + failure);
      var files = new FaultFiles();
      var identity = ControllerTestSupport.identity(0);
      var config = new LogConfig(128, 128, 16);
      QuorumStateStore.format(directory, identity, files);
      try (var state = QuorumStateStore.open(directory, identity, files);
          var generation = GenerationStore.open(state, files, config)) {
        var snapshots = new SnapshotStore(directory, identity, files, state, 65536);
        for (int i = 1; i <= 12; i++) {
          generation.log().append(1, List.of(new QuorumEntry.ReadBarrier(1)));
          generation.log().flush();
          generation.checkpointCommit(i);
          if (i == 8 || i == 12) snapshots.create(new MetadataImage(i, List.of()), 1);
        }
        files.failAfter(failure);
        assertThrows(java.io.IOException.class, () -> generation.retainPrefix(8));
        files.clearFailure();
      }
      try (var state = QuorumStateStore.open(directory, identity, files);
          var generation = GenerationStore.open(state, files, config)) {
        assertEquals(12, generation.recoveredImage().appliedOffset());
        assertEquals(12, generation.committedOffset());
      }
    }
  }
}

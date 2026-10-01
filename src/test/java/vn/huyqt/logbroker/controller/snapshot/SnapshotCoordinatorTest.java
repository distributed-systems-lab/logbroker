package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;

class SnapshotCoordinatorTest {
  @Test
  void triggerUsesBytesSinceCapturedSnapshotAndKeepsOneImageInFlight() {
    var effects = new ArrayList<QuorumEffect.DiskEffect>();
    var published = new ArrayList<SnapshotId>();
    var c =
        new SnapshotCoordinator(
            100, new UUID(0, 1), () -> 3, effects::add, published::add, () -> List.of());
    c.onApplied(new MetadataImage(10, List.of()), 2, 100);
    c.onApplied(new MetadataImage(11, List.of()), 2, 150);
    assertEquals(1, effects.size());
    var create = (QuorumEffect.CreateSnapshot) effects.getFirst();
    assertEquals(10, create.image().appliedOffset());
    assertTrue(c.retainedBytes() > 0);
    var id = new SnapshotId(10, 2, new UUID(0, 10));
    c.onCreated(id);
    assertEquals(List.of(id), published);
    assertEquals(1, effects.size());
    assertEquals(0, c.retainedBytes());
    c.onApplied(new MetadataImage(12, List.of()), 2, 200);
    assertEquals(2, effects.size());
    c.close();
  }

  @Test
  void controlRecordsAlsoTriggerAndCreationDoesNotReplaceCapturedBoundary() {
    var effects = new ArrayList<QuorumEffect.DiskEffect>();
    var c =
        new SnapshotCoordinator(
            1, new UUID(0, 1), () -> 1, effects::add, ignored -> {}, () -> List.of());
    c.onApplied(new MetadataImage(1, List.of()), 1, 1);
    c.onApplied(new MetadataImage(2, List.of()), 1, 2);
    assertEquals(1, ((QuorumEffect.CreateSnapshot) effects.getFirst()).image().appliedOffset());
    c.onCreated(new SnapshotId(1, 1, new UUID(0, 2)));
    assertEquals(2, effects.size());
    c.close();
  }
}

package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.support.QuorumHarness;

class DivergenceTest {
  @Test
  void incompatibleUncommittedTailIsReplacedAtWholeBatchBoundary() {
    var h = QuorumHarness.threeNodes(2);
    h.elect(0);
    h.tick(java.time.Duration.ofMillis(200));
    h.settle();
    assertEquals(
        1,
        h.node(0).status().epoch(),
        () ->
            "Unexpected election after Begin: "
                + h.node(0).status()
                + " peers="
                + h.node(1).status()
                + " "
                + h.node(2).status());
    h.crash(2);
    var disk = h.disk(2);
    var end = disk.index().end();
    var conflicting =
        new QuorumBatch(
            end, List.of(new QuorumEntry.ReadBarrier(1), new QuorumEntry.ReadBarrier(1)));
    disk.enqueue(
        new QuorumEffect.AppendReplica(new DiskToken(99, 1, new UUID(0, 12)), conflicting));
    disk.completeNext();
    disk.recover();
    h.restart(2);
    h.tick(java.time.Duration.ofMillis(200));
    h.settle();
    // Rediscovery may require a new election because restart deliberately forgets the leader.
    h.elect(0);
    h.tick(java.time.Duration.ofMillis(200));
    h.settle();
    assertEquals(h.disk(0).batches(), disk.batches());
    h.assertSafety();
  }
}

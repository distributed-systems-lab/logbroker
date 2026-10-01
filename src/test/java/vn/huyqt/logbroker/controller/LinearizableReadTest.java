package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Consistency;
import vn.huyqt.logbroker.controller.support.QuorumHarness;

class LinearizableReadTest {
  @Test
  void readsAdmittedBeforeDrainShareBarrierButLaterReadsNeedAnother() {
    var h = QuorumHarness.threeNodes(1);
    h.elect(0);
    long deadline = h.now() + 30_000_000_000L;
    long before = h.node(0).status().logEnd();
    var one = h.service(0).readMetadata(deadline);
    var two = h.service(0).readMetadata(deadline);
    assertFalse(one.isDone());
    h.settle();
    assertEquals(Consistency.LINEARIZABLE, one.join().consistency());
    assertEquals(two.join().applied(), one.join().applied());
    assertEquals(before + 1, h.node(0).status().logEnd());
    var later = h.service(0).readMetadata(deadline);
    assertFalse(later.isDone());
    h.settle();
    assertTrue(later.join().applied() > one.join().applied());
  }

  @Test
  void isolatedLeaderCannotCompleteFreshRead() {
    var h = QuorumHarness.threeNodes(1);
    h.elect(0);
    h.isolate(0);
    var read = h.service(0).readMetadata(h.now() + 30_000_000_000L);
    h.settle();
    assertFalse(read.isDone());
    h.tick(Duration.ofSeconds(4));
    h.settle();
    assertTrue(read.isCompletedExceptionally());
  }
}

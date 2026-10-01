package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.log.QuorumEntry;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.*;

class PendingTopicLeadershipTest {
  @Test
  void truncatedAppendCompletedAfterStepdownDoesNotReserveNameForever() {
    var h = QuorumHarness.threeNodes(42);
    h.elect(0);
    h.tick(Duration.ofMillis(200));
    h.settle();
    h.isolate(0);
    h.pauseDisk(0);
    h.service(0).createTopic("discarded", 1, h.now() + Duration.ofSeconds(30).toNanos());
    h.settle();
    assertTrue(h.disk(0).pending() > 0);
    var identity = ControllerTestSupport.identity(1);
    long next = h.node(0).status().epoch() + 1;
    h.request(
        0,
        new Frame(
            (short) 102,
            false,
            identity.clusterId(),
            1,
            777,
            identity.voterHash(),
            new BeginQuorumEpoch(next)));
    h.completeOneDisk(0);
    h.resumeDisk(0);
    h.settle();
    h.elect(1);
    h.heal();
    for (int i = 0; i < 20; i++) {
      h.tick(Duration.ofMillis(100));
      h.settle();
    }
    assertTrue(
        h.disk(0).batches().stream()
            .flatMap(b -> b.entries().stream())
            .noneMatch(
                e ->
                    e instanceof QuorumEntry.Topic topic
                        && topic.event().name().equals("discarded")));
    h.elect(0);
    var retry = h.service(0).createTopic("discarded", 1, h.now() + Duration.ofSeconds(2).toNanos());
    for (int i = 0; i < 30 && !retry.isDone(); i++) {
      h.tick(Duration.ofMillis(100));
      h.settle();
    }
    assertTrue(retry.isDone());
    assertFalse(
        retry.isCompletedExceptionally(),
        "Stale reservation blocked a fresh append after reelection");
    assertNotNull(retry.join());
    h.assertSafety();
  }
}

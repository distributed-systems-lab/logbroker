package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.support.HistoryEvent;
import vn.huyqt.logbroker.controller.support.HistoryEvent.*;

class LinearizabilityHistoryTest {
  @Test
  void timedOutCreateMayCommitAfterItsTimeoutResponse() {
    var id = new UUID(0, 1);
    var topic = new vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated(id, "orders", 1);
    assertTrue(
        check(
            List.of(
                new Invocation(1, new Create("orders", 1), 0),
                new Completion(1, new Unknown(), 1),
                new Invocation(2, new Read(), 2),
                new Completion(2, new Topics(List.of()), 3),
                new Invocation(3, new Read(), 4),
                new Completion(3, new Topics(List.of(topic)), 5))));
  }

  @Test
  void checkerRejectsOldBarrierReuseAfterCompletedCreate() {
    UUID id = new UUID(0, 1);
    assertFalse(
        check(
            List.of(
                new Invocation(1, new Create("orders", 1), 0),
                new Completion(1, new Created(id), 1),
                new Invocation(2, new Read(), 2),
                new Completion(2, new Topics(List.of()), 3))));
  }

  @Test
  void checkerAcceptsConcurrentReadBeforeCreateAndOptionalTimedOutCreate() {
    UUID id = new UUID(0, 1);
    var topic = new vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated(id, "orders", 1);
    assertTrue(
        check(
            List.of(
                new Invocation(1, new Create("orders", 1), 0),
                new Invocation(2, new Read(), 1),
                new Completion(1, new Created(id), 2),
                new Completion(2, new Topics(List.of()), 3))));
    assertTrue(
        check(
            List.of(
                new Invocation(1, new Create("orders", 1), 0),
                new Completion(1, new Unknown(), 1),
                new Invocation(2, new Read(), 2),
                new Completion(2, new Topics(List.of(topic)), 3))));
  }

  static boolean check(List<HistoryEvent> history) {
    return HistoryChecker.check(history);
  }
}

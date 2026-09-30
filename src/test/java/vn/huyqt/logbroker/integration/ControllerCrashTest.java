package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.support.ThreeControllerProcesses;

class ControllerCrashTest {
  @TempDir Path root;

  @Test
  void acknowledgedCreateSurvivesLeaderKillAndMinorityCannotReadLinearizably() throws Exception {
    try (var cluster = new ThreeControllerProcesses(root)) {
      cluster.startAll();
      int leader = cluster.awaitLeader(Duration.ofSeconds(20));
      UUID id = cluster.client().createTopic("survivor", 2).get(30, TimeUnit.SECONDS);
      cluster.kill(leader);
      assertNotEquals(leader, cluster.awaitLeader(Duration.ofSeconds(20)));
      assertEquals(id, cluster.client().createTopic("survivor", 2).get(30, TimeUnit.SECONDS));
      cluster.restart(leader);
      cluster.awaitConvergence(Duration.ofSeconds(20));
      leader = cluster.awaitLeader(Duration.ofSeconds(20));
      cluster.isolate(leader);
      cluster.assertMinorityBarrierFails(leader, Duration.ofSeconds(5));
      cluster.heal();
      cluster.awaitLeader(Duration.ofSeconds(20));
      cluster.awaitConvergence(Duration.ofSeconds(20));
    }
  }
}

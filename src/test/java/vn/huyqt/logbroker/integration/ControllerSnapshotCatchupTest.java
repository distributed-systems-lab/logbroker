package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.controller.support.ThreeControllerProcesses;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason =
                "Strict controller durability requires directory force; run on Linux/WSL ext4")
class ControllerSnapshotCatchupTest {
    @TempDir Path root;

    @Test
    void offlineFollowerInstallsSnapshotAfterRealPrefixDeletion() throws Exception {
        try (var cluster = new ThreeControllerProcesses(root)) {
            cluster.startAll();
            int leader = cluster.awaitLeader(Duration.ofSeconds(20));
            int follower = (leader + 1) % 3;
            UUID oldGeneration =
                    cluster.client().describe(follower).get(5, TimeUnit.SECONDS).generation();
            cluster.kill(follower);
            var ids = new HashMap<String, UUID>();
            for (int i = 0; i < 24; i++)
                ids.put(
                        "topic-" + i,
                        cluster.client().createTopic("topic-" + i, 1).get(30, TimeUnit.SECONDS));
            cluster.awaitPrefixDeletion(Duration.ofSeconds(20));
            cluster.restart(follower);
            cluster.awaitConvergence(Duration.ofSeconds(30));
            var status = cluster.client().describe(follower).get(5, TimeUnit.SECONDS);
            assertNotEquals(oldGeneration, status.generation());
            assertTrue(status.snapshotEnd() > 0);
            var view = cluster.client().localMetadata(follower).get(5, TimeUnit.SECONDS);
            assertEquals(24, view.topics().size());
            for (var topic : view.topics()) assertEquals(ids.get(topic.name()), topic.id());
        }
    }
}

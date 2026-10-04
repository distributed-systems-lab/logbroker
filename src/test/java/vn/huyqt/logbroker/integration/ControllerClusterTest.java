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
class ControllerClusterTest {
    @TempDir Path root;

    @Test
    void threeStrictProcessesCreateReadAndRestartAll() throws Exception {
        try (var cluster = new ThreeControllerProcesses(root)) {
            cluster.startAll();
            cluster.awaitLeader(Duration.ofSeconds(20));
            UUID id = cluster.client().createTopic("orders", 3).get(30, TimeUnit.SECONDS);
            assertEquals(
                    id,
                    cluster.client().metadata().get(30, TimeUnit.SECONDS).topics().getFirst().id());
            cluster.awaitConvergence(Duration.ofSeconds(20));
            for (int i = 0; i < 3; i++) cluster.kill(i);
            for (int i = 0; i < 3; i++) cluster.restart(i);
            cluster.awaitLeader(Duration.ofSeconds(20));
            assertEquals(id, cluster.client().createTopic("orders", 3).get(30, TimeUnit.SECONDS));
            cluster.awaitConvergence(Duration.ofSeconds(20));
        }
    }
}

package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.*;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.integration.support.Phase4Processes;
import vn.huyqt.logbroker.protocol.*;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "Strict six-process acceptance requires Linux/ext4 directory force")
class ClusterProvisioningCrashTest {
    @Test
    void missingCompleteLogStaysUnavailableWhileOtherPartitionsRecover(@TempDir Path root)
            throws Exception {
        try (var cluster = Phase4Processes.start(root)) {
            var topic = cluster.createTopic("orders", 6);
            assertEquals(1, cluster.owner(topic, 0));
            cluster.produceFlushed(topic, 0, new byte[] {1});
            cluster.produceFlushed(topic, 3, new byte[] {3});
            cluster.killBroker(1);
            var partitionRoot =
                    cluster.data(1)
                            .resolve("partitions")
                            .resolve(topic.toString())
                            .resolve("0")
                            .toAbsolutePath()
                            .normalize();
            assertTrue(partitionRoot.startsWith(root.toAbsolutePath().normalize()));
            try (var files = Files.list(partitionRoot)) {
                var logs = files.filter(p -> p.getFileName().toString().endsWith(".log")).toList();
                assertFalse(logs.isEmpty());
                for (var log : logs) Files.delete(log);
            }
            cluster.restartBroker(1);
            cluster.awaitRunning(1, Duration.ofSeconds(30));
            assertArrayEquals(new byte[] {3}, cluster.fetchValues(topic, 3).getFirst());
            var response = cluster.fetchDirect(1, topic, 0);
            assertNotEquals(ErrorCode.NONE, response.results().getFirst().error().code());
            try (var files = Files.list(partitionRoot)) {
                assertTrue(files.noneMatch(p -> p.getFileName().toString().endsWith(".log")));
            }
        }
    }
}

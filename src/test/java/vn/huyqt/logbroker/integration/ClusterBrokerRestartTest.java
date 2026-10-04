package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.*;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.integration.support.Phase4Processes;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "Strict six-process acceptance requires Linux/ext4 directory force")
class ClusterBrokerRestartTest {
    @Test
    void wholeClusterRestartPreservesUuidEpochLineageAndFlushedContent(@TempDir Path root)
            throws Exception {
        try (var cluster = Phase4Processes.start(root)) {
            var topic = cluster.createTopic("orders", 6);
            var before =
                    cluster.client()
                            .refresh(System.nanoTime() + Duration.ofSeconds(10).toNanos())
                            .get();
            for (int partition = 0; partition < 6; partition++)
                cluster.produceFlushed(topic, partition, new byte[] {(byte) partition});
            for (int id = 1; id <= 3; id++) cluster.killBroker(id);
            for (int node = 0; node < 3; node++) cluster.killController(node);
            for (int node = 0; node < 3; node++) cluster.controllers().restart(node);
            cluster.controllers().awaitLeader(Duration.ofSeconds(20));
            for (int id = 1; id <= 3; id++) {
                cluster.restartBroker(id);
                cluster.awaitRunning(id, Duration.ofSeconds(30));
            }
            assertEquals(topic, cluster.createTopic("orders", 6));
            var after =
                    cluster.client()
                            .refresh(System.nanoTime() + Duration.ofSeconds(10).toNanos())
                            .get();
            assertTrue(after.appliedOffset() > before.appliedOffset());
            for (var old : before.brokers())
                assertTrue(
                        after.brokers().stream()
                                        .filter(b -> b.id() == old.id())
                                        .findFirst()
                                        .orElseThrow()
                                        .brokerEpoch()
                                > old.brokerEpoch());
            for (int partition = 0; partition < 6; partition++) {
                var values = cluster.fetchValues(topic, partition);
                assertEquals(1, values.size());
                assertArrayEquals(new byte[] {(byte) partition}, values.getFirst());
                assertEquals(1, cluster.produceFlushed(topic, partition, new byte[] {9}));
            }
        }
    }
}

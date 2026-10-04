package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.*;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.integration.support.Phase4Processes;

import java.nio.file.Path;
import java.util.*;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "Strict six-process acceptance requires Linux/ext4 directory force")
class ClusterRoutingTest {
    @Test
    void oneBootstrapRoutesAllSixPartitionsToThreeProductionBrokers(@TempDir Path root)
            throws Exception {
        try (var cluster = Phase4Processes.start(root)) {
            var topic = cluster.createTopic("orders", 6);
            var owners = new HashSet<Integer>();
            for (int partition = 0; partition < 6; partition++) {
                owners.add(cluster.owner(topic, partition));
                assertEquals(
                        0, cluster.produceFlushed(topic, partition, new byte[] {(byte) partition}));
                var values = cluster.fetchValues(topic, partition);
                assertEquals(1, values.size());
                assertArrayEquals(new byte[] {(byte) partition}, values.getFirst());
            }
            assertEquals(Set.of(1, 2, 3), owners);
        }
    }
}

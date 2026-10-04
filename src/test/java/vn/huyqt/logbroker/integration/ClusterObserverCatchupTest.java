package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.*;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.integration.support.Phase4Processes;

import java.nio.file.Path;
import java.time.Duration;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "Strict six-process acceptance requires Linux/ext4 directory force")
class ClusterObserverCatchupTest {
    @Test
    void deletedPrefixRequiresNewObserverGenerationWithIdenticalAssignments(@TempDir Path root)
            throws Exception {
        try (var cluster = Phase4Processes.start(root)) {
            var topic = cluster.createTopic("orders", 6);
            cluster.produceFlushed(topic, 2, new byte[] {42});
            var old = cluster.observerStatus(3);
            cluster.killBroker(3);
            for (int index = 0; index < 12; index++) cluster.createTopic("catchup-" + index, 2);
            cluster.controllers().awaitPrefixDeletion(Duration.ofSeconds(20));
            int leader = cluster.controllers().awaitLeader(Duration.ofSeconds(10));
            assertTrue(
                    cluster.controllers().client().describe(leader).get().snapshotEnd()
                            > Long.parseLong(old[1]));
            cluster.restartBroker(3);
            cluster.awaitRunning(3, Duration.ofSeconds(30));
            cluster.awaitObserverSnapshot(3, Duration.ofSeconds(20));
            var restored = cluster.observerStatus(3);
            assertNotEquals(old[2], restored[2]);
            assertTrue(Long.parseLong(restored[3]) > 0);
            var expected = cluster.metadataFrom(1);
            var actual = cluster.metadataFrom(3);
            assertEquals(expected.clusterId(), actual.clusterId());
            assertEquals(expected.brokers(), actual.brokers());
            assertEquals(expected.topics(), actual.topics());
            assertArrayEquals(new byte[] {42}, cluster.fetchValues(topic, 2).getFirst());
        }
    }
}

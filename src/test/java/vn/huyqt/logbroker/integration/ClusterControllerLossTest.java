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
class ClusterControllerLossTest {
    @Test
    void warmedDataSurvivesControlIsolationButRestartNeedsFreshRecovery(@TempDir Path root)
            throws Exception {
        try (var cluster = Phase4Processes.start(root)) {
            var topic = cluster.createTopic("orders", 6);
            assertEquals(1, cluster.owner(topic, 0));
            assertEquals(0, cluster.produceFlushed(topic, 0, new byte[] {1}));
            cluster.disconnectControllersFromBrokers();
            cluster.awaitControlIsolation(Duration.ofSeconds(12));
            assertEquals(1, cluster.produceFlushed(topic, 0, new byte[] {2}));
            assertEquals(2, cluster.fetchValues(topic, 0).size());
            cluster.killBroker(1);
            cluster.restartBroker(1);
            assertFalse(cluster.becomesRunning(1, Duration.ofSeconds(12)));
            cluster.heal();
            cluster.awaitRunning(1, Duration.ofSeconds(30));
            cluster.awaitRunning(2, Duration.ofSeconds(30));
            cluster.awaitRunning(3, Duration.ofSeconds(30));
            assertEquals(2, cluster.fetchValues(topic, 0).size());
        }
    }

    @Test
    void lossOfVoterMajorityDoesNotRevokeWarmedRf1Data(@TempDir Path root) throws Exception {
        try (var cluster = Phase4Processes.start(root)) {
            var topic = cluster.createTopic("orders", 6);
            cluster.produceFlushed(topic, 0, new byte[] {1});
            int leader = cluster.controllers().awaitLeader(Duration.ofSeconds(10));
            for (int node = 0; node < 3; node++) if (node != leader) cluster.killController(node);
            cluster.awaitControlIsolationAfterMajorityLoss(Duration.ofSeconds(3));
            assertEquals(1, cluster.produceFlushed(topic, 0, new byte[] {2}));
            assertEquals(2, cluster.fetchValues(topic, 0).size());
            var command =
                    cluster.client()
                            .request(
                                    new vn.huyqt.logbroker.protocol.Protocol.CreateTopic(
                                            "uncommitted", 1),
                                    System.nanoTime() + Duration.ofMillis(700).toNanos());
            assertThrows(
                    java.util.concurrent.ExecutionException.class,
                    () -> command.get(2, java.util.concurrent.TimeUnit.SECONDS));
        }
    }
}

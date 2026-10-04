package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerMainTest {
    @TempDir Path directory;

    @Test
    void statusLineReportsStateAndBudgetsWithoutRecordPayloads() {
        var identity =
                new vn.huyqt.logbroker.broker.cluster.BrokerIdentityStore.Identity(
                        java.util.UUID.randomUUID(), 7, java.util.UUID.randomUUID());
        var status =
                new vn.huyqt.logbroker.broker.cluster.BrokerStatus(
                        identity,
                        null,
                        vn.huyqt.logbroker.broker.cluster.BrokerLifecycle.State.RECOVERING,
                        1,
                        42,
                        11,
                        12,
                        8,
                        java.util.UUID.randomUUID(),
                        java.util.Map.of("ready", 2),
                        java.util.Map.of("requestBytes", 4096L),
                        java.util.Map.of());
        String line = BrokerMain.statusLine(status);
        assertTrue(line.contains("state=RECOVERING"));
        assertTrue(line.contains("appliedOffset=11"));
        assertTrue(line.contains("durableOffset=12"));
        assertTrue(line.contains("requestBytes=4096"));
    }

    @Test
    void explicitClusterFormatPublishesManifestAndRequiresClusterConfig() throws Exception {
        var properties = directory.resolve("cluster.properties");
        Files.writeString(
                properties,
                "cluster.id=00000000-0000-0000-0000-000000000001\nbroker.id=7\nadvertised.host=localhost\nadvertised.port=9092\ncontroller.bootstrap.servers=localhost:19090\n");
        var root = directory.resolve("cluster-root");
        var identity =
                BrokerMain.format(
                        new String[] {"--config", properties.toString(), "--data", root.toString()},
                        new vn.huyqt.logbroker.controller.support.FaultFiles());
        assertEquals(7, identity.brokerId());
        assertTrue(Files.isRegularFile(root.resolve("broker-identity.bin")));
        assertTrue(Files.isRegularFile(root.resolve("observer/observer-state.journal")));
        assertTrue(Files.isRegularFile(root.resolve("partition-inventory.journal")));
        assertEquals(
                identity.clusterId(),
                BrokerMain.clusterConfig(new String[] {"--config", properties.toString()})
                        .clusterId());
        assertThrows(
                IllegalArgumentException.class,
                () -> BrokerMain.main(new String[] {"--data", root.toString()}));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        BrokerMain.format(
                                new String[] {"--data", root.toString()},
                                new vn.huyqt.logbroker.controller.support.FaultFiles()));
    }

    @Test
    void configFileAndCliOverridesApplyInOrder() throws Exception {
        var properties = directory.resolve("broker.properties");
        Files.writeString(properties, "port=19092\nflushIntervalMs=25\nmaxConnections=4\n");
        var config =
                BrokerMain.parse(
                        new String[] {
                            "--data",
                            directory.resolve("data").toString(),
                            "--config",
                            properties.toString(),
                            "--port",
                            "0"
                        });
        assertEquals(0, config.port());
        assertEquals(25, config.flushInterval().toMillis());
        assertEquals(4, config.maxConnections());
    }

    @Test
    void rejectsUnknownProperty() throws Exception {
        var properties = directory.resolve("broker.properties");
        Files.writeString(properties, "unboundedQueue=true\n");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        BrokerMain.parse(
                                new String[] {
                                    "--data",
                                    directory.toString(),
                                    "--config",
                                    properties.toString()
                                }));
    }
}

package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.support.FaultFiles;

class ClusterBrokerLifecycleTest {
    @TempDir Path root;
    @Test void unformattedRootIsRejectedWithoutCreatingLocalMetadata() {
        assertThrows(java.io.IOException.class,()->Broker.start(BrokerConfig.defaults(root).withPort(0),BrokerControlClientTest.config()));
        assertFalse(Files.exists(root.resolve("metadata"))); assertFalse(Files.exists(root.resolve("broker-identity.bin")));
    }
    @Test void fencedListenerKeepsRootLockUntilShutdownAndNeverCreatesLocalMetadata() throws Exception {
        var files=new FaultFiles(); var cluster=BrokerControlClientTest.config();
        BrokerIdentityStore.format(root,cluster.clusterId(),cluster.brokerId(),files);
        var factory=new ScriptedControlTransport.Factory();
        try(var broker=Broker.start(BrokerConfig.defaults(root).withPort(0),cluster,files,factory)) {
            assertTrue(broker.address().getPort()>0); assertFalse(broker.canServe());
            assertThrows(java.io.IOException.class,()->BrokerIdentityStore.open(root,cluster.clusterId(),cluster.brokerId(),files));
            assertFalse(Files.exists(root.resolve("metadata")));
            broker.shutdown(java.time.Duration.ofSeconds(5)).get(6,TimeUnit.SECONDS);
        }
        try(var reopened=BrokerIdentityStore.open(root,cluster.clusterId(),cluster.brokerId(),files)) {
            assertEquals(cluster.brokerId(),reopened.identity().brokerId());
        }
    }
    @Test void listenerBindFailureClosesStorageBeforeReleasingRoot() throws Exception {
        var files = new FaultFiles();
        var cluster = BrokerControlClientTest.config();
        BrokerIdentityStore.format(root, cluster.clusterId(), cluster.brokerId(), files);
        try (var occupied = new java.net.ServerSocket(0)) {
            assertThrows(java.io.IOException.class, () -> Broker.start(
                    BrokerConfig.defaults(root).withPort(occupied.getLocalPort()), cluster, files,
                    new ScriptedControlTransport.Factory()));
            try (var reopened = BrokerIdentityStore.open(root, cluster.clusterId(), cluster.brokerId(), files)) {
                assertEquals(cluster.brokerId(), reopened.identity().brokerId());
            }
        }
        assertFalse(Files.exists(root.resolve("metadata")));
    }
}

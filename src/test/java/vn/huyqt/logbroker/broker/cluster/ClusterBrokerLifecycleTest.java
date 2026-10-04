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
            var status = broker.status();
            assertEquals(cluster.brokerId(), status.identity().brokerId());
            assertNotNull(status.observerGeneration());
            assertEquals(0, status.appliedOffset());
            assertEquals(-1, status.heartbeatAgeMillis());
            assertEquals(0L, status.budgetUsage().get("requestContexts"));
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
    @Test void realListenerServesV2BootstrapAndRejectsV1() throws Exception {
        var files = new FaultFiles(); var cluster = BrokerControlClientTest.config();
        BrokerIdentityStore.format(root, cluster.clusterId(), cluster.brokerId(), files);
        var codec = new vn.huyqt.logbroker.protocol.ProtocolCodec(vn.huyqt.logbroker.protocol.ProtocolLimits.defaults());
        try (var broker = Broker.start(BrokerConfig.defaults(root).withPort(0), cluster, files, new ScriptedControlTransport.Factory());
             var socket = new java.net.Socket(broker.address().getAddress(), broker.address().getPort())) {
            socket.setSoTimeout(3000);
            for (short version : new short[]{2, 1}) {
                vn.huyqt.logbroker.protocol.Protocol.Request body = version == 2
                        ? new vn.huyqt.logbroker.protocol.ClusterProtocol.Metadata(new java.util.UUID(0, 0), java.util.List.of())
                        : new vn.huyqt.logbroker.protocol.Protocol.Metadata(java.util.List.of());
                socket.getOutputStream().write(codec.encodeRequest(new vn.huyqt.logbroker.protocol.Protocol.RequestFrame((short) 2, version, version, body)));
                var input = new java.io.DataInputStream(socket.getInputStream()); int length = input.readInt();
                byte[] raw = new byte[length + 4]; java.nio.ByteBuffer.wrap(raw).putInt(length); input.readFully(raw, 4, length);
                var response = codec.decodeResponse(raw); assertEquals(version, response.version());
                if (version == 2) assertEquals(cluster.clusterId(), assertInstanceOf(
                        vn.huyqt.logbroker.protocol.ClusterProtocol.MetadataReply.class, response.body()).clusterId());
                else assertEquals(vn.huyqt.logbroker.protocol.ErrorCode.UNSUPPORTED_VERSION,
                        assertInstanceOf(vn.huyqt.logbroker.protocol.Protocol.Failure.class, response.body()).error().code());
            }
        }
    }
}

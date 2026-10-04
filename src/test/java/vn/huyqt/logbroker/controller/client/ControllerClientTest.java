package vn.huyqt.logbroker.controller.client;

import static org.junit.jupiter.api.Assertions.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;
import vn.huyqt.logbroker.support.ManualScheduler;

class ControllerClientTest {
    @Test
    void schemaTwoAdminRequestsCarryRoleAndExposeFullClusterMetadata() throws Exception {
        var identity = new ClusterIdentity(ID.clusterId(), 0, ID.voters(), (short) 2);
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client = new ControllerClient(identity, BOOT, clock, factory)) {
            var future = client.clusterMetadata();
            assertEquals(2, factory.last().sent.version());
            assertEquals(BrokerControlProtocol.SenderRole.ADMIN, factory.last().sent.senderRole());
            var image =
                    new vn.huyqt.logbroker.controller.metadata.MetadataImage(
                            1, List.of(), (short) 2, Map.of(), Map.of());
            factory.last()
                    .reply(
                            new BrokerControlProtocol.MetadataReply(
                                    FakeClientTransport.ok(),
                                    Consistency.LINEARIZABLE,
                                    0,
                                    1,
                                    image));
            assertEquals(image, future.get(2, TimeUnit.SECONDS).image());
        }
    }

    @Test
    void boundsActiveRequestsAndCancellationReleasesCorrelation() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client = new ControllerClient(ID, BOOT, clock, factory)) {
            var calls = new ArrayList<CompletableFuture<?>>();
            for (int i = 0; i < 1024; i++) calls.add(client.metadata());
            var rejected = client.metadata();
            var error =
                    assertInstanceOf(
                            ControllerClientException.class,
                            assertThrows(
                                            ExecutionException.class,
                                            () -> rejected.get(2, TimeUnit.SECONDS))
                                    .getCause());
            assertEquals(QuorumError.OVERLOADED, error.error());
            for (var call : calls) call.cancel(false);
            assertEquals(0, clock.pending());
            assertTrue(factory.transports.stream().allMatch(wire -> wire.closed));
        }
    }

    static final ClusterIdentity ID = ControllerTestSupport.identity(0);
    static final List<InetSocketAddress> BOOT = List.of(new InetSocketAddress("localhost", 19090));

    @Test
    void successUsesStableUuidAndIgnoresStaleCorrelation() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client = new ControllerClient(ID, BOOT, clock, factory)) {
            var future = client.createTopic("orders", 3);
            var wire = factory.last();
            UUID uuid = UUID.randomUUID();
            wire.reply(
                    new CreateTopicReply(FakeClientTransport.ok(), uuid),
                    wire.sent.requestId() + 1,
                    ID.clusterId());
            assertFalse(future.isDone());
            wire.reply(new CreateTopicReply(FakeClientTransport.ok(), uuid));
            assertEquals(uuid, future.get(2, TimeUnit.SECONDS));
            assertEquals(0, clock.pending());
            assertTrue(wire.closed);
        }
    }

    @Test
    void rejectsWrongClusterBootstrap() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client = new ControllerClient(ID, BOOT, clock, factory)) {
            var future = client.metadata();
            factory.last()
                    .reply(
                            new Failure(FakeClientTransport.ok()),
                            factory.last().sent.requestId(),
                            UUID.randomUUID());
            var error =
                    assertInstanceOf(
                            ControllerClientException.class,
                            assertThrows(
                                            ExecutionException.class,
                                            () -> future.get(2, TimeUnit.SECONDS))
                                    .getCause());
            assertEquals(QuorumError.CLUSTER_MISMATCH, error.error());
        }
    }

    @Test
    void closeFailsPendingAndRemovesTimer() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        var client = new ControllerClient(ID, BOOT, clock, factory);
        var future = client.metadata();
        client.close();
        assertInstanceOf(
                ControllerClientException.class,
                assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS))
                        .getCause());
        assertEquals(0, clock.pending());
        assertTrue(factory.last().closed);
    }
}

final class FakeClientTransport implements ControllerClientTransport {
    Consumer<Frame> receive;
    Consumer<Throwable> failure;
    Frame sent;
    boolean closed;
    boolean refuseConnect;

    static ReplyMeta ok() {
        return new ReplyMeta(QuorumError.NONE, "", 1, 0);
    }

    public CompletableFuture<Void> connect(
            InetSocketAddress address, Consumer<Frame> receive, Consumer<Throwable> failure) {
        this.receive = receive;
        this.failure = failure;
        return refuseConnect
                ? CompletableFuture.failedFuture(new java.io.IOException("connect refused"))
                : CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> send(Frame frame) {
        sent = frame;
        return CompletableFuture.completedFuture(null);
    }

    void reply(Reply reply) {
        reply(reply, sent.requestId(), sent.clusterId());
    }

    void reply(Reply reply, long requestId, UUID cluster) {
        receive.accept(
                new Frame(
                        sent.version(),
                        BrokerControlProtocol.SenderRole.VOTER,
                        sent.operation(),
                        true,
                        cluster,
                        0,
                        requestId,
                        sent.version() == 2
                                ? ControllerTestSupport.identity(0).voterHash()
                                : sent.voterHash(),
                        reply));
    }

    void acceptThenLoseResponse() {
        failure.accept(new java.io.IOException("accepted but response lost"));
    }

    void replyNotLeader() {
        reply(new Failure(new ReplyMeta(QuorumError.NOT_LEADER, "", 2, 1)));
    }

    public void close() {
        closed = true;
    }

    static final class Factory implements ControllerClientTransport.Factory {
        final List<FakeClientTransport> transports = new ArrayList<>();
        boolean refuse;

        public ControllerClientTransport create() {
            var wire = new FakeClientTransport();
            wire.refuseConnect = refuse;
            transports.add(wire);
            return wire;
        }

        FakeClientTransport last() {
            return transports.getLast();
        }
    }
}

package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.protocol.*;

class ClusterClientRoutingTest {
    @Test void metadataNeverRegressesAndChangedEndpointsReplaceConnections() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0)));
            var refresh = h.client().refresh(30_000_000_000L); h.runDue();
            var topic = new ClusterProtocol.TopicInfo("orders", ClusterClientHarness.TOPIC, List.of(
                    new ClusterProtocol.PartitionInfo(0, Protocol.Error.none(), List.of(1), 1, 2, 2)));
            var moved = new ClusterProtocol.BrokerInfo(1,
                    new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint("127.0.0.1", 19092), 2, false);
            h.reply(1, new ClusterProtocol.MetadataReply(Protocol.Error.none(), ClusterClientHarness.CLUSTER, 20, List.of(moved), List.of(topic)));
            assertEquals(20, refresh.join().appliedOffset());
            var request = h.produceToPartition(0);
            var produce = h.client().request(request, 30_000_000_000L); h.runDue();
            var routed = (ClusterProtocol.Produce) h.requests(2).getLast();
            assertEquals(2, routed.entries().getFirst().route().brokerEpoch());
            assertEquals(2, routed.entries().getFirst().route().leaderEpoch());
            h.reply(2, success(request, 5)); assertTrue(produce.isDone());
            var stale = h.client().refresh(30_000_000_000L); h.runDue();
            h.replyMetadata(Map.of(1, List.of(0)));
            assertEquals(20, stale.join().appliedOffset());
            h.client().request(request, 30_000_000_000L); h.runDue();
            assertInstanceOf(ClusterProtocol.Produce.class, h.requests(2).getLast());
        }
    }
    @Test void unknownTopicRefreshesButSameNameNeverReplacesTheRequestedUuid() {
        try (var h = new ClusterClientHarness()) {
            h.client().refresh(30_000_000_000L); h.runDue();
            h.reply(1, new ClusterProtocol.MetadataReply(Protocol.Error.none(), ClusterClientHarness.CLUSTER, 0, List.of(), List.of()));
            var request = h.produceToPartition(0);
            var result = h.client().request(request, 250_000_000L); h.runDue();
            h.clock.advance(java.time.Duration.ofMillis(100)); h.runDue();
            var broker = new ClusterProtocol.BrokerInfo(1,
                    new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint("127.0.0.1", 19091), 1, false);
            var replacement = new ClusterProtocol.TopicInfo("orders", new UUID(0, 8), List.of(
                    new ClusterProtocol.PartitionInfo(0, Protocol.Error.none(), List.of(1), 1, 1, 1)));
            h.reply(1, new ClusterProtocol.MetadataReply(Protocol.Error.none(), ClusterClientHarness.CLUSTER, 10, List.of(broker), List.of(replacement)));
            h.clock.advance(java.time.Duration.ofMillis(150)); h.runDue();
            assertEquals(ClusterProtocol.Outcome.REJECTED, ((ClusterProtocol.ProduceReply) result.join()).results().getFirst().outcome());
            assertEquals(0, h.requests(1).stream().filter(ClusterProtocol.Produce.class::isInstance).count());
        }
    }
    @Test void publicCompletionsDoNotHoldTheRoutingMonitor() {
        try (var h = new ClusterClientHarness()) {
            var refresh = h.client().refresh(30_000_000_000L);
            var metadataCallback = refresh.thenRun(() -> assertFalse(Thread.holdsLock(h.client())));
            h.runDue(); h.replyMetadata(Map.of(1, List.of(0)));
            metadataCallback.join();
            var request = h.produceToPartition(0);
            var result = h.client().request(request, 30_000_000_000L);
            var produceCallback = result.thenRun(() -> assertFalse(Thread.holdsLock(h.client())));
            h.runDue(); h.reply(1, success(request, 0)); produceCallback.join();
            var pending = h.client().request(request, 30_000_000_000L);
            var closeCallback = pending.thenRun(() -> assertFalse(Thread.holdsLock(h.client())));
            h.runDue(); h.client().close(); closeCallback.join();
        }
    }
    @Test void silentBootstrapFailsOverWithinTheOriginalDeadline() {
        try (var h = new ClusterClientHarness(1, 1024 * 1024, 8,
                List.of(new java.net.InetSocketAddress("127.0.0.1", 19091), new java.net.InetSocketAddress("127.0.0.1", 19092)), null)) {
            var result = h.client().refresh(5_000_000_000L); h.runDue();
            h.clock.advance(java.time.Duration.ofSeconds(2)); h.runDue();
            h.clock.advance(java.time.Duration.ofMillis(100)); h.runDue();
            assertEquals(1, h.requests(2).size());
            h.reply(2, new ClusterProtocol.MetadataReply(Protocol.Error.none(), ClusterClientHarness.CLUSTER, 0, List.of(), List.of()));
            assertEquals(ClusterClientHarness.CLUSTER, result.join().clusterId());
        }
    }
    @Test void operationAndByteLimitsReleaseOnCancelAndCloseCompletesPending() {
        var bootstrap = List.of(new java.net.InetSocketAddress("127.0.0.1", 19091));
        try (var h = new ClusterClientHarness(1, 2048, 1, bootstrap, ClusterClientHarness.CLUSTER)) {
            var first = h.client().request(h.produceToPartition(0), 30_000_000_000L); h.runDue();
            assertTrue(h.client().request(h.produceToPartition(0), 30_000_000_000L).isCompletedExceptionally());
            first.cancel(false); h.runDue();
            var next = h.client().request(h.produceToPartition(0), 30_000_000_000L); h.runDue();
            assertFalse(next.isDone()); h.client().close(); h.runDue(); assertTrue(next.isDone());
        }
        try (var h = new ClusterClientHarness(1, 1, 8, bootstrap, ClusterClientHarness.CLUSTER)) {
            assertTrue(h.client().request(h.produceToPartition(0), 30_000_000_000L).isCompletedExceptionally());
            h.runDue(); assertTrue(h.requests(1).isEmpty());
        }
    }
    @Test void splitsByCommittedOwnerAndMergesInInputOrder() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1)));
            var first = h.produceToPartition(1); var second = h.produceToPartition(0);
            var result = h.client().request(new ClusterProtocol.Produce(first.clusterId(), first.ack(), 30000,
                    List.of(first.entries().getFirst(), second.entries().getFirst())), 30_000_000_000L);
            h.runDue();
            assertEquals(1, h.requests(2).stream().filter(ClusterProtocol.Produce.class::isInstance).count());
            h.reply(1, success(second, 10)); h.reply(2, success(first, 20));
            var reply = assertInstanceOf(ClusterProtocol.ProduceReply.class, result.join());
            assertEquals(20, reply.results().getFirst().firstOffset()); assertEquals(10, reply.results().getLast().firstOffset());
        }
    }
    static ClusterProtocol.ProduceReply success(ClusterProtocol.Produce request, long offset) {
        return new ClusterProtocol.ProduceReply(Protocol.Error.none(), List.of(new ClusterProtocol.ProduceResult(
                request.entries().getFirst().route().partition(), Protocol.Error.none(), ClusterProtocol.Outcome.SUCCESS, offset, offset + 1)));
    }
    @Test void refreshIsSingleFlightAndPinsClusterIdentity() {
        try (var h = new ClusterClientHarness()) {
            var first = h.client().refresh(30_000_000_000L); var second = h.client().refresh(30_000_000_000L); h.runDue();
            assertSame(first, second); assertEquals(1, h.requests(1).size());
            h.reply(1, new ClusterProtocol.MetadataReply(Protocol.Error.none(), new UUID(0, 99), 0, List.of(), List.of()));
            assertTrue(first.isCompletedExceptionally());
        }
    }
}

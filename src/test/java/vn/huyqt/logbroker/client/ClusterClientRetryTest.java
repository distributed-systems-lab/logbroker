package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.protocol.*;

import java.util.*;

class ClusterClientRetryTest {
    @Test
    void partialUnknownPreservesSuccessfulOffsetsWithoutRetry() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1)));
            var a = h.produceToPartition(0);
            var b = h.produceToPartition(1);
            var result =
                    h.client()
                            .request(
                                    new ClusterProtocol.Produce(
                                            a.clusterId(),
                                            a.ack(),
                                            30000,
                                            List.of(
                                                    a.entries().getFirst(),
                                                    b.entries().getFirst())),
                                    30_000_000_000L);
            h.runDue();
            h.reply(1, ClusterClientRoutingTest.success(a, 10));
            h.failAfterSend(2);
            var reply = (ClusterProtocol.ProduceReply) result.join();
            assertEquals(10, reply.results().getFirst().firstOffset());
            assertEquals(ClusterProtocol.Outcome.UNKNOWN, reply.results().getLast().outcome());
            h.clock.advance(java.time.Duration.ofSeconds(1));
            h.runDue();
            assertEquals(
                    1,
                    h.requests(2).stream()
                            .filter(ClusterProtocol.Produce.class::isInstance)
                            .count());
        }
    }

    @Test
    void originalDeadlineIsNotResetByMetadataRefreshOrRetry() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0)));
            var result = h.client().request(h.produceToPartition(0), 150_000_000L);
            h.runDue();
            h.reply(
                    1,
                    new ClusterProtocol.ProduceReply(
                            Protocol.Error.none(),
                            List.of(
                                    new ClusterProtocol.ProduceResult(
                                            new Protocol.TopicPartition(
                                                    ClusterClientHarness.TOPIC, 0),
                                            new Protocol.Error(ErrorCode.OVERLOADED, "busy"),
                                            ClusterProtocol.Outcome.REJECTED,
                                            -1,
                                            -1))));
            h.clock.advance(java.time.Duration.ofMillis(100));
            h.runDue();
            h.clock.advance(java.time.Duration.ofMillis(50));
            h.runDue();
            assertEquals(
                    ClusterProtocol.Outcome.REJECTED,
                    ((ClusterProtocol.ProduceReply) result.join()).results().getFirst().outcome());
            assertEquals(
                    1,
                    h.requests(1).stream()
                            .filter(ClusterProtocol.Produce.class::isInstance)
                            .count());
        }
    }

    @Test
    void createTimeoutAfterSendRetainsUnknownOutcome() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0)));
            var result =
                    h.client()
                            .request(
                                    new ClusterProtocol.CreateTopic(
                                            ClusterClientHarness.CLUSTER,
                                            "next",
                                            1,
                                            (short) 1,
                                            100),
                                    100_000_000L);
            h.runDue();
            h.clock.advance(java.time.Duration.ofMillis(100));
            h.runDue();
            var error = assertThrows(java.util.concurrent.CompletionException.class, result::join);
            assertEquals(
                    ClientException.Outcome.UNKNOWN,
                    assertInstanceOf(ClientException.class, error.getCause()).outcome());
        }
    }

    @Test
    void lostProduceResponseIsNotRetried() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1)));
            var result = h.client().request(h.produceToPartition(0), 30_000_000_000L);
            h.runDue();
            h.failAfterSend(1);
            h.runDue();
            var reply = assertInstanceOf(ClusterProtocol.ProduceReply.class, result.join());
            assertEquals(ClusterProtocol.Outcome.UNKNOWN, reply.results().getFirst().outcome());
            assertEquals(
                    1,
                    h.requests(1).stream()
                            .filter(ClusterProtocol.Produce.class::isInstance)
                            .count());
        }
    }

    @Test
    void onlyRejectedEntriesRetryAndSuccessIsRetained() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1)));
            var a = h.produceToPartition(0);
            var b = h.produceToPartition(1);
            var result =
                    h.client()
                            .request(
                                    new ClusterProtocol.Produce(
                                            a.clusterId(),
                                            a.ack(),
                                            30000,
                                            List.of(
                                                    a.entries().getFirst(),
                                                    b.entries().getFirst())),
                                    30_000_000_000L);
            h.runDue();
            h.reply(1, ClusterClientRoutingTest.success(a, 10));
            h.reply(
                    2,
                    new ClusterProtocol.ProduceReply(
                            Protocol.Error.none(),
                            List.of(
                                    new ClusterProtocol.ProduceResult(
                                            b.entries().getFirst().route().partition(),
                                            new Protocol.Error(
                                                    ErrorCode.STALE_PARTITION_EPOCH, "stale"),
                                            ClusterProtocol.Outcome.REJECTED,
                                            -1,
                                            -1))));
            h.clock.advance(java.time.Duration.ofMillis(100));
            h.runDue();
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1)));
            h.runDue();
            h.reply(2, ClusterClientRoutingTest.success(b, 20));
            var reply = (ClusterProtocol.ProduceReply) result.join();
            assertEquals(10, reply.results().getFirst().firstOffset());
            assertEquals(
                    1,
                    h.requests(1).stream()
                            .filter(ClusterProtocol.Produce.class::isInstance)
                            .count());
            assertEquals(
                    2,
                    h.requests(2).stream()
                            .filter(ClusterProtocol.Produce.class::isInstance)
                            .count());
        }
    }
}

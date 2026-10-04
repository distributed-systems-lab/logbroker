package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;

import java.util.*;

class ClusterFetchBudgetTest {
    @Test
    void onlyOneBrokerReceivesOversizedExceptionAndItConsumesTheGlobalBudget() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1), 3, List.of(2)));
            var entries = new ArrayList<ClusterProtocol.FetchEntry>();
            for (int i = 0; i < 3; i++)
                entries.add(
                        new ClusterProtocol.FetchEntry(
                                new ClusterProtocol.Route(
                                        new Protocol.TopicPartition(ClusterClientHarness.TOPIC, i),
                                        i + 1,
                                        1,
                                        1),
                                0,
                                1));
            var result =
                    h.client()
                            .request(
                                    new ClusterProtocol.Fetch(
                                            ClusterClientHarness.CLUSTER, 1, 0, 0, entries, true),
                                    30_000_000_000L);
            h.runDue();
            var sent = assertInstanceOf(ClusterProtocol.Fetch.class, h.requests(1).getLast());
            assertTrue(sent.allowOversizedFirstBatch());
            var batch =
                    new Protocol.FetchBatch(
                            0,
                            new Protocol.Batch(
                                    List.of(new LogRecord(0, null, new byte[] {1}, List.of()))));
            h.reply(
                    1,
                    new ClusterProtocol.FetchReply(
                            Protocol.Error.none(),
                            List.of(
                                    new ClusterProtocol.FetchResult(
                                            entries.getFirst().route().partition(),
                                            Protocol.Error.none(),
                                            0,
                                            1,
                                            1,
                                            List.of(batch)))));
            assertTrue(result.isDone());
            assertEquals(
                    0,
                    h.requests(2).stream().filter(ClusterProtocol.Fetch.class::isInstance).count());
            assertEquals(
                    0,
                    h.requests(3).stream().filter(ClusterProtocol.Fetch.class::isInstance).count());
            var reply = assertInstanceOf(ClusterProtocol.FetchReply.class, result.join());
            assertEquals(3, reply.results().size());
            assertEquals(1, reply.results().stream().mapToInt(r -> r.batches().size()).sum());
        }
    }

    @Test
    void aggregateMinimumDoesNotMultiplyAcrossBrokersAndOffsetsStayUnchanged() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(Map.of(1, List.of(0), 2, List.of(1)));
            var entries =
                    List.of(
                            new Protocol.FetchEntry(
                                    new Protocol.TopicPartition(ClusterClientHarness.TOPIC, 0),
                                    7,
                                    4096),
                            new Protocol.FetchEntry(
                                    new Protocol.TopicPartition(ClusterClientHarness.TOPIC, 1),
                                    7,
                                    4096));
            var result =
                    h.client().request(new Protocol.Fetch(4096, 1, 5000, entries), 30_000_000_000L);
            h.runDue();
            var a = assertInstanceOf(ClusterProtocol.Fetch.class, h.requests(1).getLast());
            assertEquals(0, a.minBytes());
            h.reply(1, empty(entries.getFirst().partition()));
            var b = assertInstanceOf(ClusterProtocol.Fetch.class, h.requests(2).getLast());
            assertEquals(0, b.minBytes());
            h.reply(2, empty(entries.getLast().partition()));
            assertFalse(result.isDone());
            h.clock.advance(java.time.Duration.ofMillis(100));
            h.runDue();
            var next = assertInstanceOf(ClusterProtocol.Fetch.class, h.requests(1).getLast());
            assertEquals(7, next.entries().getFirst().offset());
            h.reply(
                    1,
                    new ClusterProtocol.FetchReply(
                            Protocol.Error.none(),
                            List.of(
                                    new ClusterProtocol.FetchResult(
                                            entries.getFirst().partition(),
                                            Protocol.Error.none(),
                                            0,
                                            8,
                                            8,
                                            List.of(
                                                    new Protocol.FetchBatch(
                                                            7,
                                                            new Protocol.Batch(
                                                                    List.of(
                                                                            new LogRecord(
                                                                                    0,
                                                                                    null,
                                                                                    new byte[] {1},
                                                                                    List
                                                                                            .of())))))))));
            h.reply(2, empty(entries.getLast().partition()));
            assertTrue(result.isDone());
        }
    }

    static ClusterProtocol.FetchReply empty(Protocol.TopicPartition partition) {
        return new ClusterProtocol.FetchReply(
                Protocol.Error.none(),
                List.of(
                        new ClusterProtocol.FetchResult(
                                partition, Protocol.Error.none(), 0, 7, 7, List.of())));
    }
}

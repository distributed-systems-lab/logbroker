package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.LoopbackTransport;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

class ProducerTest {
    @Test
    void clusterProducerKeepsItsLaneBusyThroughRoutingRetry() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(java.util.Map.of(1, List.of(0)));
            var base = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 19091));
            var config =
                    new ClientConfig(
                            base.address(),
                            base.queuedBytes(),
                            32,
                            1,
                            java.time.Duration.ZERO,
                            base.requestTimeout());
            try (var producer = new Producer(h.client(), config, h.clock)) {
                var first =
                        producer.send(
                                "orders",
                                0,
                                new LogRecord(0, null, new byte[] {1}, List.of()),
                                Protocol.AckMode.APPENDED);
                h.runDue();
                h.replyMetadata(java.util.Map.of(1, List.of(0)));
                h.runDue();
                var second =
                        producer.send(
                                "orders",
                                0,
                                new LogRecord(0, null, new byte[] {2}, List.of()),
                                Protocol.AckMode.APPENDED);
                h.runDue();
                h.reply(
                        1,
                        new vn.huyqt.logbroker.protocol.ClusterProtocol.ProduceReply(
                                Protocol.Error.none(),
                                List.of(
                                        new vn.huyqt.logbroker.protocol.ClusterProtocol
                                                .ProduceResult(
                                                new Protocol.TopicPartition(
                                                        ClusterClientHarness.TOPIC, 0),
                                                new Protocol.Error(
                                                        vn.huyqt.logbroker.protocol.ErrorCode
                                                                .OVERLOADED,
                                                        "busy"),
                                                vn.huyqt.logbroker.protocol.ClusterProtocol.Outcome
                                                        .REJECTED,
                                                -1,
                                                -1))));
                assertFalse(first.isDone());
                assertFalse(second.isDone());
                assertEquals(
                        1,
                        h.requests(1).stream()
                                .filter(
                                        vn.huyqt.logbroker.protocol.ClusterProtocol.Produce.class
                                                ::isInstance)
                                .count());
                h.clock.advance(java.time.Duration.ofMillis(100));
                h.runDue();
                h.replyMetadata(java.util.Map.of(1, List.of(0)));
                h.reply(1, ClusterClientRoutingTest.success(h.produceToPartition(0), 10));
                assertEquals(10, first.join().offset());
                assertFalse(second.isDone());
                h.reply(1, ClusterClientRoutingTest.success(h.produceToPartition(0), 11));
                assertEquals(11, second.join().offset());
            }
        }
    }

    @Test
    void groupsRecordsByPartitionAndAckThenCompletesIndividualOffsets() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        var topicId = UUID.randomUUID();
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config, transport, clock);
                var producer = new Producer(client, config, clock)) {
            var first =
                    producer.send(
                            "orders",
                            0,
                            new LogRecord(0, null, new byte[] {1}, List.of()),
                            Protocol.AckMode.APPENDED);
            var metadata = transport.sent().getFirst();
            transport.reply(
                    new Protocol.ResponseFrame(
                            (short) 2,
                            (short) 1,
                            metadata.requestId(),
                            new Protocol.MetadataReply(
                                    Protocol.Error.none(),
                                    "localhost",
                                    9092,
                                    List.of(
                                            new Protocol.TopicInfo(
                                                    "orders",
                                                    topicId,
                                                    List.of(
                                                            new Protocol.PartitionInfo(
                                                                    0, Protocol.Error.none())))))));
            var second =
                    producer.send(
                            "orders",
                            0,
                            new LogRecord(0, null, new byte[] {2}, List.of()),
                            Protocol.AckMode.APPENDED);
            clock.advance(Duration.ofMillis(6));
            clock.runDue();
            var sent = transport.sent().getLast();
            var produce = (Protocol.Produce) sent.body();
            assertEquals(2, produce.entries().getFirst().batch().records().size());
            transport.reply(
                    new Protocol.ResponseFrame(
                            (short) 3,
                            (short) 1,
                            sent.requestId(),
                            new Protocol.ProduceReply(
                                    Protocol.Error.none(),
                                    List.of(
                                            new Protocol.ProduceResult(
                                                    new Protocol.TopicPartition(topicId, 0),
                                                    Protocol.Error.none(),
                                                    7,
                                                    9)))));
            assertEquals(7, first.join().offset());
            assertEquals(8, second.join().offset());
        }
    }

    @Test
    void differentAckModesPreservePerPartitionSendOrder() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        var topicId = UUID.randomUUID();
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config, transport, clock);
                var producer = new Producer(client, config, clock)) {
            var first =
                    producer.send(
                            "orders",
                            0,
                            new LogRecord(0, null, new byte[] {1}, List.of()),
                            Protocol.AckMode.APPENDED);
            replyMetadata(transport, topicId, 1);
            var second =
                    producer.send(
                            "orders",
                            0,
                            new LogRecord(0, null, new byte[] {2}, List.of()),
                            Protocol.AckMode.FLUSHED);
            assertEquals(2, transport.sent().size());
            var firstFrame = transport.sent().getLast();
            assertEquals(Protocol.AckMode.APPENDED, ((Protocol.Produce) firstFrame.body()).ack());
            clock.advance(Duration.ofMillis(6));
            clock.runDue();
            assertEquals(2, transport.sent().size());
            replyProduce(transport, firstFrame, topicId, 0, 0, 1);
            assertEquals(3, transport.sent().size());
            var secondFrame = transport.sent().getLast();
            assertEquals(Protocol.AckMode.FLUSHED, ((Protocol.Produce) secondFrame.body()).ack());
            replyProduce(transport, secondFrame, topicId, 0, 1, 2);
            assertEquals(0, first.join().offset());
            assertEquals(1, second.join().offset());
        }
    }

    @Test
    void nullKeysShareCurrentBatchAndAdvanceAfterSeal() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        var topicId = UUID.randomUUID();
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config, transport, clock);
                var producer = new Producer(client, config, clock)) {
            var first =
                    producer.send(
                            "orders",
                            null,
                            new LogRecord(0, null, new byte[] {1}, List.of()),
                            Protocol.AckMode.APPENDED);
            replyMetadata(transport, topicId, 2);
            var second =
                    producer.send(
                            "orders",
                            null,
                            new LogRecord(0, null, new byte[] {2}, List.of()),
                            Protocol.AckMode.APPENDED);
            clock.advance(Duration.ofMillis(6));
            clock.runDue();
            var firstBatch = transport.sent().getLast();
            var firstEntry = ((Protocol.Produce) firstBatch.body()).entries().getFirst();
            assertEquals(0, firstEntry.partition().partition());
            assertEquals(2, firstEntry.batch().records().size());
            replyProduce(transport, firstBatch, topicId, 0, 0, 2);
            assertEquals(0, first.join().partition().partition());
            assertEquals(0, second.join().partition().partition());
            var third =
                    producer.send(
                            "orders",
                            null,
                            new LogRecord(0, null, new byte[] {3}, List.of()),
                            Protocol.AckMode.APPENDED);
            clock.advance(Duration.ofMillis(6));
            clock.runDue();
            var secondBatch = transport.sent().getLast();
            assertEquals(
                    1,
                    ((Protocol.Produce) secondBatch.body())
                            .entries()
                            .getFirst()
                            .partition()
                            .partition());
            replyProduce(transport, secondBatch, topicId, 1, 0, 1);
            assertEquals(1, third.join().partition().partition());
        }
    }

    @Test
    void closeSealsAndWaitsForAcceptedBatchResponse() throws Exception {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        var topicId = UUID.randomUUID();
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config, transport, clock)) {
            var producer = new Producer(client, config, clock);
            var accepted =
                    producer.send(
                            "orders",
                            0,
                            new LogRecord(0, null, new byte[] {1}, List.of()),
                            Protocol.AckMode.FLUSHED);
            replyMetadata(transport, topicId, 1);
            var closing = java.util.concurrent.CompletableFuture.runAsync(producer::close);
            long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
            while (transport.sent().size() < 2 && System.nanoTime() < until) Thread.sleep(5);
            assertEquals(2, transport.sent().size());
            assertFalse(closing.isDone());
            replyProduce(transport, transport.sent().getLast(), topicId, 0, 0, 1);
            closing.get(2, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(0, accepted.join().offset());
        }
    }

    private static void replyMetadata(LoopbackTransport transport, UUID id, int count) {
        var partitions = new java.util.ArrayList<Protocol.PartitionInfo>();
        for (int i = 0; i < count; i++)
            partitions.add(new Protocol.PartitionInfo(i, Protocol.Error.none()));
        var request = transport.sent().getFirst();
        transport.reply(
                new Protocol.ResponseFrame(
                        (short) 2,
                        (short) 1,
                        request.requestId(),
                        new Protocol.MetadataReply(
                                Protocol.Error.none(),
                                "localhost",
                                9092,
                                List.of(new Protocol.TopicInfo("orders", id, partitions)))));
    }

    private static void replyProduce(
            LoopbackTransport transport,
            Protocol.RequestFrame request,
            UUID id,
            int partition,
            long first,
            long next) {
        transport.reply(
                new Protocol.ResponseFrame(
                        (short) 3,
                        (short) 1,
                        request.requestId(),
                        new Protocol.ProduceReply(
                                Protocol.Error.none(),
                                List.of(
                                        new Protocol.ProduceResult(
                                                new Protocol.TopicPartition(id, partition),
                                                Protocol.Error.none(),
                                                first,
                                                next)))));
    }
}

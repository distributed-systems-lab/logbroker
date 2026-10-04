package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.LoopbackTransport;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

class ConsumerTest {
    @Test
    void clusterConsumerFiltersAtRequestedOffsetWithoutAdvancingItImplicitly() {
        try (var h = new ClusterClientHarness()) {
            h.replyMetadata(java.util.Map.of(1, List.of(0)));
            var consumer = new Consumer(h.client());
            var partition = new Protocol.TopicPartition(ClusterClientHarness.TOPIC, 0);
            var pending =
                    consumer.fetch(
                            List.of(new Protocol.FetchEntry(partition, 11, 1024)), 1024, 0, 0);
            h.runDue();
            h.reply(
                    1,
                    new vn.huyqt.logbroker.protocol.ClusterProtocol.FetchReply(
                            Protocol.Error.none(),
                            List.of(
                                    new vn.huyqt.logbroker.protocol.ClusterProtocol.FetchResult(
                                            partition,
                                            Protocol.Error.none(),
                                            0,
                                            12,
                                            12,
                                            List.of(
                                                    new Protocol.FetchBatch(
                                                            10,
                                                            new Protocol.Batch(
                                                                    List.of(
                                                                            new LogRecord(
                                                                                    0,
                                                                                    null,
                                                                                    new byte[] {1},
                                                                                    List.of()),
                                                                            new LogRecord(
                                                                                    0,
                                                                                    null,
                                                                                    new byte[] {2},
                                                                                    List
                                                                                            .of())))))))));
            assertEquals(11, pending.join().getFirst().records().getFirst().offset());
            assertEquals(1, pending.join().getFirst().records().size());
        }
    }

    @Test
    void filtersRecordsBeforeRequestedOffsetWithinWholeBatch() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config, transport, clock)) {
            var consumer = new Consumer(client);
            var partition = new Protocol.TopicPartition(UUID.randomUUID(), 0);
            var result =
                    consumer.fetch(
                            List.of(new Protocol.FetchEntry(partition, 11, 1024)), 1024, 0, 0);
            var sent = transport.sent().getFirst();
            var batch =
                    new Protocol.Batch(
                            List.of(
                                    new LogRecord(0, null, new byte[] {1}, List.of()),
                                    new LogRecord(0, null, new byte[] {2}, List.of())));
            transport.reply(
                    new Protocol.ResponseFrame(
                            (short) 4,
                            (short) 1,
                            sent.requestId(),
                            new Protocol.FetchReply(
                                    Protocol.Error.none(),
                                    List.of(
                                            new Protocol.FetchResult(
                                                    partition,
                                                    Protocol.Error.none(),
                                                    0,
                                                    12,
                                                    List.of(
                                                            new Protocol.FetchBatch(
                                                                    10, batch)))))));
            assertEquals(1, result.join().getFirst().records().size());
            assertEquals(11, result.join().getFirst().records().getFirst().offset());
        }
    }
}

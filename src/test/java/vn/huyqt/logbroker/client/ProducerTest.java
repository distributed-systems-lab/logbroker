package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.LoopbackTransport;
import vn.huyqt.logbroker.support.ManualScheduler;

class ProducerTest {
    @Test void groupsRecordsByPartitionAndAckThenCompletesIndividualOffsets() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        var topicId = UUID.randomUUID();
        try (var clock = new ManualScheduler();
             var client = new BrokerClient(config, transport, clock);
             var producer = new Producer(client, config, clock)) {
            var first = producer.send("orders", 0, new LogRecord(0, null,
                    new byte[]{1}, List.of()), Protocol.AckMode.APPENDED);
            var metadata = transport.sent().getFirst();
            transport.reply(new Protocol.ResponseFrame((short) 2, (short) 1, metadata.requestId(),
                    new Protocol.MetadataReply(Protocol.Error.none(), "localhost", 9092,
                            List.of(new Protocol.TopicInfo("orders", topicId,
                                    List.of(new Protocol.PartitionInfo(0, Protocol.Error.none())))))));
            var second = producer.send("orders", 0, new LogRecord(0, null,
                    new byte[]{2}, List.of()), Protocol.AckMode.APPENDED);
            clock.advance(Duration.ofMillis(6)); clock.runDue();
            var sent = transport.sent().getLast();
            var produce = (Protocol.Produce) sent.body();
            assertEquals(2, produce.entries().getFirst().batch().records().size());
            transport.reply(new Protocol.ResponseFrame((short) 3, (short) 1, sent.requestId(),
                    new Protocol.ProduceReply(Protocol.Error.none(), List.of(
                            new Protocol.ProduceResult(new Protocol.TopicPartition(topicId, 0),
                                    Protocol.Error.none(), 7, 9)))));
            assertEquals(7, first.join().offset());
            assertEquals(8, second.join().offset());
        }
    }
}

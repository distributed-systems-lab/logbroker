package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.LoopbackTransport;
import vn.huyqt.logbroker.support.ManualScheduler;

class ConsumerTest {
    @Test void filtersRecordsBeforeRequestedOffsetWithinWholeBatch() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        try (var clock = new ManualScheduler();
             var client = new BrokerClient(config, transport, clock)) {
            var consumer = new Consumer(client);
            var partition = new Protocol.TopicPartition(UUID.randomUUID(), 0);
            var result = consumer.fetch(List.of(new Protocol.FetchEntry(partition, 11, 1024)),
                    1024, 0, 0);
            var sent = transport.sent().getFirst();
            var batch = new Protocol.Batch(List.of(
                    new LogRecord(0, null, new byte[]{1}, List.of()),
                    new LogRecord(0, null, new byte[]{2}, List.of())));
            transport.reply(new Protocol.ResponseFrame((short) 4, (short) 1, sent.requestId(),
                    new Protocol.FetchReply(Protocol.Error.none(), List.of(new Protocol.FetchResult(
                            partition, Protocol.Error.none(), 0, 12,
                            List.of(new Protocol.FetchBatch(10, batch)))))));
            assertEquals(1, result.join().getFirst().records().size());
            assertEquals(11, result.join().getFirst().records().getFirst().offset());
        }
    }
}

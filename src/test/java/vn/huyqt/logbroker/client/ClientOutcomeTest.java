package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.support.LoopbackTransport;
import vn.huyqt.logbroker.support.ManualScheduler;

class ClientOutcomeTest {
    @Test void lostProduceResponseHasUnknownOutcomeAndIsNotRetried() {
        var transport = new LoopbackTransport();
        var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
        try (var clock = new ManualScheduler();
             var client = new BrokerClient(config, transport, clock)) {
            var partition = new Protocol.TopicPartition(UUID.randomUUID(), 0);
            var pending = client.request(new Protocol.Produce(Protocol.AckMode.FLUSHED, 1000,
                    List.of(new Protocol.ProduceEntry(partition,
                            new Protocol.Batch(List.of(new LogRecord(0, null,
                                    new byte[]{1}, List.of())))))));
            transport.failConnection(new IOException("lost response"));
            var failure = assertThrows(java.util.concurrent.CompletionException.class, pending::join);
            var cause = assertInstanceOf(ClientException.class, failure.getCause());
            assertEquals(ClientException.Outcome.UNKNOWN, cause.outcome());
            assertEquals(1, transport.sent().size());
        }
    }
}

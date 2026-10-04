package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.support.LoopbackTransport;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

class BrokerClientTest {
    private static ClientConfig config() {
        return ClientConfig.defaults(new InetSocketAddress("127.0.0.1", 9092));
    }

    @Test
    void correlatesResponsesArrivingOutOfOrder() {
        var transport = new LoopbackTransport();
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config(), transport, clock)) {
            var one = client.request(new Protocol.CreateTopic("one", 1));
            var two = client.request(new Protocol.CreateTopic("two", 1));
            var sent = transport.sent();
            assertEquals(2, sent.size());
            transport.reply(
                    new Protocol.ResponseFrame(
                            (short) 1,
                            (short) 1,
                            sent.get(1).requestId(),
                            new Protocol.CreateTopicReply(
                                    Protocol.Error.none(), UUID.randomUUID())));
            assertTrue(two.isDone());
            assertFalse(one.isDone());
            transport.reply(
                    new Protocol.ResponseFrame(
                            (short) 1,
                            (short) 1,
                            sent.get(0).requestId(),
                            new Protocol.CreateTopicReply(
                                    Protocol.Error.none(), UUID.randomUUID())));
            assertTrue(one.isDone());
        }
    }

    @Test
    void timeoutDoesNotMatchLateResponseToNewRequest() {
        var transport = new LoopbackTransport();
        try (var clock = new ManualScheduler();
                var client = new BrokerClient(config(), transport, clock)) {
            var old = client.request(new Protocol.Metadata(List.of("old")));
            long oldId = transport.sent().getFirst().requestId();
            clock.advance(Duration.ofSeconds(31));
            clock.runDue();
            assertTrue(old.isCompletedExceptionally());
            var fresh = client.request(new Protocol.Metadata(List.of("new")));
            transport.reply(
                    new Protocol.ResponseFrame(
                            (short) 2,
                            (short) 1,
                            oldId,
                            new Protocol.MetadataReply(
                                    Protocol.Error.none(), "localhost", 9092, List.of())));
            assertFalse(fresh.isDone());
            assertNotEquals(oldId, transport.sent().getLast().requestId());
        }
    }
}

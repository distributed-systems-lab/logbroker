package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.Broker;
import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.client.BrokerClient;
import vn.huyqt.logbroker.client.ClientConfig;
import vn.huyqt.logbroker.client.ClientException;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

class BrokerNetworkFaultTest {
    @TempDir Path directory;

    @Test void droppedProduceResponseIsUnknownWithoutAutomaticReplay() throws Exception {
        try (var broker = Broker.start(BrokerConfig.defaults(directory).withPort(0));
             var listener = new ServerSocket(0);
             var clock = DeadlineScheduler.system()) {
            int brokerPort = broker.address().getPort();
            Protocol.TopicPartition partition;
            try (var setup = client(brokerPort, clock)) {
                var created = (Protocol.CreateTopicReply) setup.request(
                        new Protocol.CreateTopic("orders", 1)).get(5, TimeUnit.SECONDS);
                partition = new Protocol.TopicPartition(created.topicId(), 0);
            }
            var forwarded = new AtomicInteger();
            var relay = CompletableFuture.runAsync(() -> {
                try (var incoming = listener.accept();
                     var upstream = new Socket("127.0.0.1", brokerPort)) {
                    incoming.setSoTimeout(5000); upstream.setSoTimeout(5000);
                    var request = readFrame(new DataInputStream(incoming.getInputStream()));
                    forwarded.incrementAndGet();
                    upstream.getOutputStream().write(request);
                    readFrame(new DataInputStream(upstream.getInputStream()));
                    // Broker completed the write. Lose only its response to this client.
                } catch (Exception error) { throw new RuntimeException(error); }
            });
            try (var client = client(listener.getLocalPort(), clock)) {
                var future = client.request(new Protocol.Produce(Protocol.AckMode.FLUSHED, 5000,
                        List.of(new Protocol.ProduceEntry(partition, new Protocol.Batch(List.of(
                                new LogRecord(0, null, new byte[]{9}, List.of())))))));
                var failure = assertThrows(CompletionException.class,
                        () -> future.orTimeout(5, TimeUnit.SECONDS).join());
                assertEquals(ClientException.Outcome.UNKNOWN,
                        assertInstanceOf(ClientException.class, failure.getCause()).outcome());
            }
            relay.get(5, TimeUnit.SECONDS);
            assertEquals(1, forwarded.get());
            try (var check = client(brokerPort, clock)) {
                var reply = (Protocol.FetchReply) check.request(new Protocol.Fetch(1024, 0, 0,
                        List.of(new Protocol.FetchEntry(partition, 0, 1024))))
                        .get(5, TimeUnit.SECONDS);
                assertEquals(1, reply.results().getFirst().batches().getFirst().batch()
                        .records().size());
            }
        }
    }

    private static BrokerClient client(int port, DeadlineScheduler clock) {
        return new BrokerClient(ClientConfig.defaults(new InetSocketAddress("127.0.0.1", port)),
                new NettyClientTransport(ProtocolLimits.defaults()), clock);
    }

    private static byte[] readFrame(DataInputStream input) throws Exception {
        int length = input.readInt();
        if (length < 12 || length > 8 * 1024 * 1024)
            throw new IllegalStateException("Invalid frame length");
        byte[] frame = new byte[length + 4];
        java.nio.ByteBuffer.wrap(frame).putInt(length);
        input.readFully(frame, 4, length);
        return frame;
    }
}

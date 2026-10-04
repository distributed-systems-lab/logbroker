package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.LegacyBrokerFixture;
import vn.huyqt.logbroker.client.BrokerClient;
import vn.huyqt.logbroker.client.ClientConfig;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class BrokerCrashTest {
    @TempDir Path directory;

    @Test
    void flushedProduceSurvivesForcedProcessDeath() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        Process child =
                new ProcessBuilder(
                                java,
                                "-cp",
                                classpath,
                                "vn.huyqt.logbroker.broker.LegacyBrokerMain",
                                "--data",
                                directory.toString(),
                                "--port",
                                "0")
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start();
        UUID id;
        try {
            var ready =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new BufferedReader(
                                                    new InputStreamReader(child.getInputStream()))
                                            .readLine();
                                } catch (Exception error) {
                                    throw new RuntimeException(error);
                                }
                            });
            String line = ready.get(10, TimeUnit.SECONDS);
            assertNotNull(line);
            assertTrue(line.startsWith("READY "), line);
            int port = Integer.parseInt(line.substring(6));
            try (var clock = DeadlineScheduler.system();
                    var client =
                            new BrokerClient(
                                    ClientConfig.defaults(new InetSocketAddress("127.0.0.1", port)),
                                    new NettyClientTransport(ProtocolLimits.defaults()),
                                    clock)) {
                var created =
                        (Protocol.CreateTopicReply)
                                client.request(new Protocol.CreateTopic("orders", 1))
                                        .get(5, TimeUnit.SECONDS);
                id = created.topicId();
                var partition = new Protocol.TopicPartition(id, 0);
                var produce =
                        (Protocol.ProduceReply)
                                client.request(
                                                new Protocol.Produce(
                                                        Protocol.AckMode.FLUSHED,
                                                        5000,
                                                        List.of(
                                                                new Protocol.ProduceEntry(
                                                                        partition,
                                                                        new Protocol.Batch(
                                                                                List.of(
                                                                                        new LogRecord(
                                                                                                0,
                                                                                                null,
                                                                                                new byte
                                                                                                        [] {
                                                                                                    7
                                                                                                },
                                                                                                List
                                                                                                        .of())))))))
                                        .get(5, TimeUnit.SECONDS);
                assertEquals(Protocol.Error.none(), produce.results().getFirst().error());
            }
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS));
        }
        LegacyBrokerFixture reopened = null;
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (reopened == null && System.nanoTime() < until) {
            try {
                reopened = LegacyBrokerFixture.start(BrokerConfig.defaults(directory).withPort(0));
            } catch (java.io.IOException lockPending) {
                Thread.sleep(25);
            }
        }
        assertNotNull(reopened, "Data-root lock did not release after process death");
        try (var broker = reopened;
                var clock = DeadlineScheduler.system();
                var client =
                        new BrokerClient(
                                ClientConfig.defaults(
                                        new InetSocketAddress(
                                                "127.0.0.1", broker.address().getPort())),
                                new NettyClientTransport(ProtocolLimits.defaults()),
                                clock)) {
            var fetched =
                    (Protocol.FetchReply)
                            client.request(
                                            new Protocol.Fetch(
                                                    1024,
                                                    0,
                                                    0,
                                                    List.of(
                                                            new Protocol.FetchEntry(
                                                                    new Protocol.TopicPartition(
                                                                            id, 0),
                                                                    0,
                                                                    1024))))
                                    .get(5, TimeUnit.SECONDS);
            assertEquals(
                    7,
                    fetched.results()
                            .getFirst()
                            .batches()
                            .getFirst()
                            .batch()
                            .records()
                            .getFirst()
                            .value()[0]);
        }
    }
}

package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import vn.huyqt.logbroker.client.BrokerClient;
import vn.huyqt.logbroker.client.ClientConfig;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

class BrokerIsolationTest {
    @TempDir Path directory;

    @Test void createsProducesFlushesAndFetchesAcrossBrokerRestart() throws Exception {
        var config = BrokerConfig.defaults(directory).withPort(0);
        java.util.UUID id;
        try (var broker = Broker.start(config);
             var clock = DeadlineScheduler.system();
             var client = new BrokerClient(ClientConfig.defaults(new InetSocketAddress(
                     "127.0.0.1", broker.address().getPort())),
                     new NettyClientTransport(ProtocolLimits.defaults()), clock)) {
            var created = (Protocol.CreateTopicReply) client.request(
                    new Protocol.CreateTopic("orders", 1)).get(5, TimeUnit.SECONDS);
            assertEquals(Protocol.Error.none(), created.error());
            id = created.topicId();
            var tp = new Protocol.TopicPartition(id, 0);
            var sent = (Protocol.ProduceReply) client.request(new Protocol.Produce(
                    Protocol.AckMode.FLUSHED, 5000, List.of(new Protocol.ProduceEntry(tp,
                    new Protocol.Batch(List.of(new LogRecord(1, null,
                            new byte[]{42}, List.of()))))))).get(5, TimeUnit.SECONDS);
            assertEquals(Protocol.Error.none(), sent.results().getFirst().error());
        }
        try (var broker = Broker.start(config);
             var clock = DeadlineScheduler.system();
             var client = new BrokerClient(ClientConfig.defaults(new InetSocketAddress(
                     "127.0.0.1", broker.address().getPort())),
                     new NettyClientTransport(ProtocolLimits.defaults()), clock)) {
            var metadata = (Protocol.MetadataReply) client.request(
                    new Protocol.Metadata(List.of("orders"))).get(5, TimeUnit.SECONDS);
            assertEquals(id, metadata.topics().getFirst().id());
            var fetched = (Protocol.FetchReply) client.request(new Protocol.Fetch(1024, 0, 0,
                    List.of(new Protocol.FetchEntry(new Protocol.TopicPartition(id, 0),
                            0, 1024)))).get(5, TimeUnit.SECONDS);
            assertEquals(42, fetched.results().getFirst().batches().getFirst().batch()
                    .records().getFirst().value()[0]);
        }
    }
}

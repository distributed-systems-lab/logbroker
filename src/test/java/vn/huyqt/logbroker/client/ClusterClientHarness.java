package vn.huyqt.logbroker.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import vn.huyqt.logbroker.protocol.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint;
import vn.huyqt.logbroker.support.*;
import vn.huyqt.logbroker.storage.LogRecord;

final class ClusterClientHarness implements AutoCloseable {
    static final UUID CLUSTER = new UUID(0, 1), TOPIC = new UUID(0, 7);
    final ManualScheduler clock = new ManualScheduler();
    final Map<Integer, List<LoopbackTransport>> wires = new HashMap<>();
    final ProtocolCodec codec = new ProtocolCodec(ProtocolLimits.defaults());
    final ClusterClient client;
    ClusterClientHarness() {
        this(3, 16 * 1024 * 1024, 32, List.of(new InetSocketAddress("127.0.0.1", 19091)), CLUSTER);
    }
    ClusterClientHarness(int maxConnections, long bytes, int inFlight, List<InetSocketAddress> bootstrap, UUID expected) {
        var address = new InetSocketAddress("127.0.0.1", 19091);
        client = new ClusterClient(new ClusterClientConfig(bootstrap, expected, maxConnections,
                Duration.ofSeconds(30), Duration.ofMillis(100), bytes, inFlight),
                ClientConfig.defaults(address), endpoint -> {
                    var wire = new LoopbackTransport();
                    wires.computeIfAbsent(endpoint.getPort() - 19090, ignored -> new ArrayList<>()).add(wire); return wire;
                }, clock);
    }
    ClusterClient client() { return client; }
    List<Protocol.Request> requests(int broker) {
        return wires.getOrDefault(broker, List.of()).stream().flatMap(wire -> wire.sent().stream()).map(Protocol.RequestFrame::body).toList();
    }
    LoopbackTransport wire(int broker) { return wires.get(broker).getLast(); }
    void runDue() { clock.runDue(); }
    void reply(int broker, Protocol.Response body) {
        var sent = wire(broker).sent().getLast();
        try { wire(broker).reply(codec.decodeResponse(codec.encodeResponse(new Protocol.ResponseFrame(
                sent.operation(), sent.version(), sent.requestId(), body)))); }
        catch (Exception error) { throw new AssertionError(error); }
        runDue();
    }
    void replyMetadata(Map<Integer, List<Integer>> ownership) {
        if (requests(1).isEmpty()) { client.refresh(30_000_000_000L); runDue(); }
        var brokers = new ArrayList<ClusterProtocol.BrokerInfo>(); var partitions = new ArrayList<ClusterProtocol.PartitionInfo>();
        ownership.forEach((broker, ids) -> {
            brokers.add(new ClusterProtocol.BrokerInfo(broker, new Endpoint("127.0.0.1", 19090 + broker), 1, false));
            for (int id : ids) partitions.add(new ClusterProtocol.PartitionInfo(id, Protocol.Error.none(), List.of(broker), broker, 1, 1));
        });
        partitions.sort(Comparator.comparingInt(ClusterProtocol.PartitionInfo::partition));
        reply(1, new ClusterProtocol.MetadataReply(Protocol.Error.none(), CLUSTER, 10, brokers,
                List.of(new ClusterProtocol.TopicInfo("orders", TOPIC, partitions))));
    }
    ClusterProtocol.Produce produceToPartition(int partition) {
        return new ClusterProtocol.Produce(CLUSTER, Protocol.AckMode.APPENDED, 30000, List.of(new ClusterProtocol.ProduceEntry(
                new ClusterProtocol.Route(new Protocol.TopicPartition(TOPIC, partition), 1, 1, 1),
                new Protocol.Batch(List.of(new LogRecord(0, null, new byte[]{1}, List.of()))))));
    }
    void failAfterSend(int broker) { wire(broker).failConnection(new java.io.IOException("response lost")); runDue(); }
    public void close() { client.close(); runDue(); clock.close(); }
}

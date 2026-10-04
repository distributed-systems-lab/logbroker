package vn.huyqt.logbroker.example;

import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.client.ClientConfig;
import vn.huyqt.logbroker.client.ClusterClient;
import vn.huyqt.logbroker.client.ClusterClientConfig;
import vn.huyqt.logbroker.client.Consumer;
import vn.huyqt.logbroker.client.Producer;
import vn.huyqt.logbroker.protocol.ClusterProtocol;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Six durable partition writes and explicit-offset reads, discovered from bootstrap brokers. */
public final class ClientExample {
    private ClientExample() {}

    /** Expects {@code <host:port[,host:port...]> <topic>}. Owns and closes all client resources. */
    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException(
                    "Usage: ClientExample <host:port[,host:port...]> <topic>");
        var result = run(args[0], args[1]);
        System.out.println("SUCCESS records=" + result.records() + " brokers=" + result.brokers());
    }

    /** Creates/reuses six RF1 partitions and verifies a FLUSHED record on each owner. */
    public static Result run(String bootstrap, String topic) throws Exception {
        var endpoints =
                Arrays.stream(bootstrap.split(",", -1))
                        .map(
                                value -> {
                                    int separator = value.lastIndexOf(':');
                                    if (separator <= 0)
                                        throw new IllegalArgumentException(
                                                "Expected host:port: " + value);
                                    return new InetSocketAddress(
                                            value.substring(0, separator),
                                            Integer.parseInt(value.substring(separator + 1)));
                                })
                        .toList();
        var wire = ClientConfig.defaults(endpoints.getFirst());
        try (var clock = DeadlineScheduler.system();
                var client =
                        new ClusterClient(
                                ClusterClientConfig.defaults(endpoints),
                                wire,
                                unused -> new NettyClientTransport(ProtocolLimits.defaults()),
                                clock);
                var producer = new Producer(client, wire, clock)) {
            long deadline = clock.nanoTime() + Duration.ofSeconds(30).toNanos();
            Protocol.Response response;
            do {
                response =
                        client.request(new Protocol.CreateTopic(topic, 6), deadline)
                                .get(35, TimeUnit.SECONDS);
                if (!(response instanceof Protocol.Failure failure)
                        || failure.error().code() != ErrorCode.NO_ELIGIBLE_BROKER) break;
                Thread.sleep(50);
            } while (clock.nanoTime() < deadline);
            if (!(response instanceof ClusterProtocol.CreateTopicReply created)
                    || created.error().code() != ErrorCode.NONE)
                throw new IllegalStateException("CreateTopic: " + response);
            // A committed controller command can precede the bootstrap observer applying it.
            // Wait for that UUID and its ready assignments before resolving names in Producer.
            deadline = clock.nanoTime() + Duration.ofSeconds(30).toNanos();
            boolean ready = false;
            do {
                var image = client.refresh(deadline).get(35, TimeUnit.SECONDS);
                ready =
                        image.topics().stream()
                                .filter(t -> t.id().equals(created.topicId()))
                                .anyMatch(
                                        t ->
                                                t.partitions().size() == 6
                                                        && t.partitions().stream()
                                                                .allMatch(
                                                                        p ->
                                                                                p.error().code()
                                                                                        == ErrorCode
                                                                                                .NONE));
                if (!ready) Thread.sleep(50);
            } while (!ready && clock.nanoTime() < deadline);
            if (!ready) throw new IllegalStateException("Topic assignments did not become ready");
            var consumer = new Consumer(client);
            int count = 0;
            for (int partition = 0; partition < 6; partition++) {
                var record =
                        new LogRecord(
                                System.currentTimeMillis(),
                                null,
                                ("hello-partition-" + partition).getBytes(StandardCharsets.UTF_8),
                                List.of());
                var result =
                        producer.send(topic, partition, record, Protocol.AckMode.FLUSHED)
                                .get(35, TimeUnit.SECONDS);
                var fetched =
                        consumer.fetch(
                                        List.of(
                                                new Protocol.FetchEntry(
                                                        result.partition(),
                                                        result.offset(),
                                                        1024 * 1024)),
                                        1024 * 1024,
                                        0,
                                        0)
                                .get(35, TimeUnit.SECONDS)
                                .getFirst();
                if (fetched.error().code() != ErrorCode.NONE
                        || fetched.records().size() != 1
                        || !record.equals(fetched.records().getFirst().record()))
                    throw new IllegalStateException(
                            "Fetched record differs on partition " + partition);
                count++;
            }
            var metadata =
                    client.refresh(clock.nanoTime() + Duration.ofSeconds(30).toNanos())
                            .get(35, TimeUnit.SECONDS);
            var owners = new HashSet<Integer>();
            metadata.topics().stream()
                    .filter(t -> t.id().equals(created.topicId()))
                    .findFirst()
                    .orElseThrow()
                    .partitions()
                    .forEach(partition -> owners.add(partition.leaderId()));
            return new Result(count, owners.size());
        }
    }

    public record Result(int records, int brokers) {}
}

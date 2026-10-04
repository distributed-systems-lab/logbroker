package vn.huyqt.logbroker.example;

import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.client.BrokerClient;
import vn.huyqt.logbroker.client.ClientConfig;
import vn.huyqt.logbroker.client.Consumer;
import vn.huyqt.logbroker.client.Producer;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * One durable Produce followed by an explicit-offset Fetch.
 *
 * <p>Runs against a broker started with {@code BrokerMain}; see {@code README.md} for the commands.
 * Prints {@code SUCCESS records=1} when the fetched record equals the produced one.
 */
public final class LegacyClientExample {
    private LegacyClientExample() {}

    /** Expects {@code <host> <port> <topic>}. */
    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IllegalArgumentException("Usage: LegacyClientExample <host> <port> <topic>");
        int count = run(args[0], Integer.parseInt(args[1]), args[2]);
        System.out.println("SUCCESS records=" + count);
    }

    /**
     * Creates {@code topic} with two partitions, produces one record to partition 0 with {@code
     * FLUSHED}, and fetches it back from the returned offset. Each step waits at most 10 seconds.
     * An existing topic with two partitions is reused.
     *
     * @return the number of records fetched, which is 1 on success
     * @throws IllegalStateException if CreateTopic returns an error, which includes the topic
     *     already existing with a different partition count, or the fetched record differs
     */
    public static int run(String host, int port, String topic) throws Exception {
        var config = ClientConfig.defaults(new InetSocketAddress(host, port));
        try (var clock = DeadlineScheduler.system();
                var client =
                        new BrokerClient(
                                config,
                                new NettyClientTransport(ProtocolLimits.defaults()),
                                clock);
                var producer = new Producer(client, config, clock)) {
            var created =
                    (Protocol.CreateTopicReply)
                            client.request(new Protocol.CreateTopic(topic, 2))
                                    .get(10, TimeUnit.SECONDS);
            if (created.error().code() != ErrorCode.NONE)
                throw new IllegalStateException("CreateTopic: " + created.error());
            var record =
                    new LogRecord(
                            System.currentTimeMillis(),
                            "sample".getBytes(StandardCharsets.UTF_8),
                            "hello-log-broker".getBytes(StandardCharsets.UTF_8),
                            List.of());
            var result =
                    producer.send(topic, 0, record, Protocol.AckMode.FLUSHED)
                            .get(10, TimeUnit.SECONDS);
            var consumer = new Consumer(client);
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
                            .get(10, TimeUnit.SECONDS);
            int count = fetched.getFirst().records().size();
            if (count != 1 || !record.equals(fetched.getFirst().records().getFirst().record()))
                throw new IllegalStateException("Fetched record differs from produced record");
            return count;
        }
    }
}

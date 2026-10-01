package vn.huyqt.logbroker.broker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogConfig;

/**
 * CLI entry point for one local broker.
 *
 * <p>Options and properties are described in {@code docs/broker-configuration.md}.
 */
public final class BrokerMain {
    private BrokerMain() {
    }

    /**
     * Starts a broker, prints {@code READY <port>} on standard output once the listener is bound,
     * and blocks until the process exits. A JVM shutdown hook closes the broker.
     */
    public static void main(String[] args) throws Exception {
        BrokerConfig config = parse(args);
        var logger = System.getLogger(BrokerMain.class.getName());
        Broker broker = Broker.start(config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                broker.close();
            } catch (RuntimeException error) {
                logger.log(System.Logger.Level.ERROR, "Broker shutdown failed", error);
            }
        }, "broker-shutdown-hook"));
        System.out.println("READY " + broker.address().getPort());
        System.out.flush();
        new CountDownLatch(1).await();
    }

    // Fail closed: unknown or duplicate options and unknown property keys abort startup instead
    // of being ignored. --host and --port override the property file.
    static BrokerConfig parse(String[] args) throws IOException {
        Path data = null, propertyFile = null;
        String host = null;
        Integer port = null;
        var seen = new HashSet<String>();
        if (args.length % 2 != 0)
            throw new IllegalArgumentException("Options need values");
        for (int i = 0; i < args.length; i += 2) {
            if (!seen.add(args[i]))
                throw new IllegalArgumentException("Duplicate option " + args[i]);
            switch (args[i]) {
                case "--data" -> data = Path.of(args[i + 1]);
                case "--config" -> propertyFile = Path.of(args[i + 1]);
                case "--host" -> host = args[i + 1];
                case "--port" -> port = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("Unknown option " + args[i]);
            }
        }
        if (data == null)
            throw new IllegalArgumentException("--data is required");
        var defaults = BrokerConfig.defaults(data);
        var properties = new Properties();
        if (propertyFile != null)
            try (var input = Files.newInputStream(propertyFile)) {
                properties.load(input);
            }
        Set<String> allowed = Set.of("host", "port", "flushIntervalMs", "flushBytes",
                "maxFetchBytes", "maxFrameBytes", "maxWireBatchBytes",
                "maxStorageBatchBytes", "maxPartitionEntries", "maxRecordsPerBatch",
                "segmentBytes", "maxBatchBytes", "indexIntervalBytes", "dataWorkers",
                "validationWorkers", "maxConnections", "maxTasksPerPartition",
                "maxValidationTasks", "maxQueuedRequestBytes",
                "maxOutboundPerConnection", "maxOutboundTotal", "maxRequestContexts",
                "maxFlushedWaiters", "maxTopics", "maxPartitions", "shutdownTimeoutMs");
        for (String key : properties.stringPropertyNames())
            if (!allowed.contains(key))
                throw new IllegalArgumentException("Unknown property " + key);
        var protocol = defaults.protocolLimits();
        protocol = new ProtocolLimits(integer(properties, "maxFrameBytes", protocol.maxFrameBytes()),
                integer(properties, "maxWireBatchBytes", protocol.maxWireBatchBytes()),
                integer(properties, "maxStorageBatchBytes", protocol.maxStorageBatchBytes()),
                integer(properties, "maxPartitionEntries", protocol.maxPartitionEntries()),
                integer(properties, "maxRecordsPerBatch", protocol.maxRecordsPerBatch()));
        var storage = defaults.logConfig();
        storage = new LogConfig(number(properties, "segmentBytes", storage.segmentBytes()),
                integer(properties, "maxBatchBytes", storage.maxBatchBytes()),
                integer(properties, "indexIntervalBytes", storage.indexIntervalBytes()));
        return new BrokerConfig(data,
                host != null ? host : properties.getProperty("host", defaults.host()),
                port != null ? port : integer(properties, "port", defaults.port()),
                Duration.ofMillis(number(properties, "flushIntervalMs",
                        defaults.flushInterval().toMillis())),
                number(properties, "flushBytes", defaults.flushBytes()),
                integer(properties, "maxFetchBytes", defaults.maxFetchBytes()),
                protocol, storage,
                integer(properties, "dataWorkers", defaults.dataWorkers()),
                integer(properties, "validationWorkers", defaults.validationWorkers()),
                integer(properties, "maxConnections", defaults.maxConnections()),
                integer(properties, "maxTasksPerPartition", defaults.maxTasksPerPartition()),
                integer(properties, "maxValidationTasks", defaults.maxValidationTasks()),
                number(properties, "maxQueuedRequestBytes", defaults.maxQueuedRequestBytes()),
                number(properties, "maxOutboundPerConnection", defaults.maxOutboundPerConnection()),
                number(properties, "maxOutboundTotal", defaults.maxOutboundTotal()),
                integer(properties, "maxRequestContexts", defaults.maxRequestContexts()),
                integer(properties, "maxFlushedWaiters", defaults.maxFlushedWaiters()),
                integer(properties, "maxTopics", defaults.maxTopics()),
                integer(properties, "maxPartitions", defaults.maxPartitions()),
                Duration.ofMillis(number(properties, "shutdownTimeoutMs",
                        defaults.shutdownTimeout().toMillis())));
    }

    private static int integer(Properties properties, String key, int fallback) {
        return Integer.parseInt(properties.getProperty(key, Integer.toString(fallback)));
    }

    private static long number(Properties properties, String key, long fallback) {
        return Long.parseLong(properties.getProperty(key, Long.toString(fallback)));
    }
}

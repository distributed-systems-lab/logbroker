package vn.huyqt.logbroker.broker;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogConfig;

/** Immutable lab limits for a single broker. */
public record BrokerConfig(Path dataDirectory, String host, int port,
                           Duration flushInterval, long flushBytes,
                           int maxFetchBytes, ProtocolLimits protocolLimits,
                           LogConfig logConfig, int dataWorkers, int validationWorkers,
                           int maxConnections, int maxTasksPerPartition,
                           int maxValidationTasks, long maxQueuedRequestBytes,
                           long maxOutboundPerConnection, long maxOutboundTotal,
                           int maxRequestContexts, int maxFlushedWaiters,
                           int maxTopics, int maxPartitions,
                           Duration shutdownTimeout) {
    public BrokerConfig {
        Objects.requireNonNull(dataDirectory); Objects.requireNonNull(host);
        Objects.requireNonNull(flushInterval); Objects.requireNonNull(protocolLimits);
        Objects.requireNonNull(logConfig); Objects.requireNonNull(shutdownTimeout);
        if (host.isBlank() || port < 0 || port > 65535 || flushInterval.isZero()
                || flushInterval.isNegative() || flushBytes <= 0 || maxFetchBytes <= 0
                || dataWorkers <= 0 || validationWorkers <= 0 || maxConnections <= 0
                || maxTasksPerPartition <= 0 || maxValidationTasks <= 0
                || maxQueuedRequestBytes <= 0 || maxOutboundPerConnection <= 0
                || maxOutboundTotal < maxOutboundPerConnection || maxRequestContexts <= 0
                || maxFlushedWaiters <= 0 || maxTopics <= 0 || maxPartitions <= 0
                || shutdownTimeout.isZero() || shutdownTimeout.isNegative()
                || maxFetchBytes > protocolLimits.maxFrameBytes() - 1024 * 1024
                || protocolLimits.maxStorageBatchBytes() > logConfig.maxBatchBytes())
            throw new IllegalArgumentException("Invalid broker configuration");
    }

    public static BrokerConfig defaults(Path dataDirectory) {
        return new BrokerConfig(dataDirectory, "127.0.0.1", 9092,
                Duration.ofMillis(10), 1024 * 1024, 4 * 1024 * 1024,
                ProtocolLimits.defaults(), LogConfig.defaults(), 4, 2, 128,
                256, 256, 64L * 1024 * 1024, 16L * 1024 * 1024,
                64L * 1024 * 1024, 1024, 4096, 128, 1024,
                Duration.ofSeconds(30));
    }

    public BrokerConfig withPort(int changedPort) {
        return copy(changedPort, flushInterval);
    }

    public BrokerConfig withFlushInterval(Duration changedInterval) {
        return copy(port, changedInterval);
    }

    private BrokerConfig copy(int changedPort, Duration changedInterval) {
        return new BrokerConfig(dataDirectory, host, changedPort, changedInterval, flushBytes,
                maxFetchBytes, protocolLimits, logConfig, dataWorkers, validationWorkers,
                maxConnections, maxTasksPerPartition, maxValidationTasks, maxQueuedRequestBytes,
                maxOutboundPerConnection, maxOutboundTotal, maxRequestContexts,
                maxFlushedWaiters, maxTopics, maxPartitions, shutdownTimeout);
    }
}

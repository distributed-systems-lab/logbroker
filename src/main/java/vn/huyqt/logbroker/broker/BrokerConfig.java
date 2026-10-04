package vn.huyqt.logbroker.broker;

import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.storage.LogConfig;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable lab limits for a single broker.
 *
 * <p>Property names, units and defaults are listed in {@code docs/broker-configuration.md}. The
 * compact constructor rejects the whole configuration with {@link IllegalArgumentException} when
 * any capacity or duration is not positive (only {@code port} may be {@code 0}, meaning ephemeral),
 * when {@code maxOutboundTotal} is below {@code maxOutboundPerConnection}, when {@code
 * maxFetchBytes} does not leave 1 MiB of the frame cap for envelope and partition metadata, or when
 * the protocol storage batch cap exceeds {@link LogConfig#maxBatchBytes()}.
 *
 * @param flushInterval maximum age of the oldest unflushed batch before a partition flush is
 *     scheduled
 * @param flushBytes unflushed encoded storage bytes per partition that trigger a flush
 * @param maxFetchBytes upper bound for a Fetch request's {@code maxBytes}, in wire batch bytes
 * @param dataWorkers threads shared by all partition lanes
 * @param validationWorkers threads the transport uses for request decoding and response encoding
 * @param maxTasksPerPartition queued user tasks per partition lane; control tasks are not counted
 * @param maxRequestContexts live requests broker-wide; the same value separately caps parked
 *     long-poll Fetch waiters
 * @param maxFlushedWaiters broker-wide {@code FLUSHED} produce entries awaiting durability
 * @param maxPartitions total partitions across all topics
 * @param shutdownTimeout deadline used by {@link Broker#close()} and by component shutdown
 */
public record BrokerConfig(
        Path dataDirectory,
        String host,
        int port,
        Duration flushInterval,
        long flushBytes,
        int maxFetchBytes,
        ProtocolLimits protocolLimits,
        LogConfig logConfig,
        int dataWorkers,
        int validationWorkers,
        int maxConnections,
        int maxTasksPerPartition,
        int maxValidationTasks,
        long maxQueuedRequestBytes,
        long maxOutboundPerConnection,
        long maxOutboundTotal,
        int maxRequestContexts,
        int maxFlushedWaiters,
        int maxTopics,
        int maxPartitions,
        Duration shutdownTimeout) {
    public BrokerConfig {
        Objects.requireNonNull(dataDirectory);
        Objects.requireNonNull(host);
        Objects.requireNonNull(flushInterval);
        Objects.requireNonNull(protocolLimits);
        Objects.requireNonNull(logConfig);
        Objects.requireNonNull(shutdownTimeout);
        if (host.isBlank()
                || port < 0
                || port > 65535
                || flushInterval.isZero()
                || flushInterval.isNegative()
                || flushBytes <= 0
                || maxFetchBytes <= 0
                || dataWorkers <= 0
                || validationWorkers <= 0
                || maxConnections <= 0
                || maxTasksPerPartition <= 0
                || maxValidationTasks <= 0
                || maxQueuedRequestBytes <= 0
                || maxOutboundPerConnection <= 0
                || maxOutboundTotal < maxOutboundPerConnection
                || maxRequestContexts <= 0
                || maxFlushedWaiters <= 0
                || maxTopics <= 0
                || maxPartitions <= 0
                || shutdownTimeout.isZero()
                || shutdownTimeout.isNegative()
                || maxFetchBytes > protocolLimits.maxFrameBytes() - 1024 * 1024
                || protocolLimits.maxStorageBatchBytes() > logConfig.maxBatchBytes())
            throw new IllegalArgumentException("Invalid broker configuration");
    }

    /**
     * Returns the Phase 2 defaults from {@code docs/broker-configuration.md}, bound to {@code
     * 127.0.0.1:9092}.
     */
    public static BrokerConfig defaults(Path dataDirectory) {
        return new BrokerConfig(
                dataDirectory,
                "127.0.0.1",
                9092,
                Duration.ofMillis(10),
                1024 * 1024,
                4 * 1024 * 1024,
                ProtocolLimits.defaults(),
                LogConfig.defaults(),
                4,
                2,
                128,
                256,
                256,
                64L * 1024 * 1024,
                16L * 1024 * 1024,
                64L * 1024 * 1024,
                1024,
                4096,
                128,
                1024,
                Duration.ofSeconds(30));
    }

    public BrokerConfig withPort(int changedPort) {
        return copy(changedPort, flushInterval);
    }

    public BrokerConfig withFlushInterval(Duration changedInterval) {
        return copy(port, changedInterval);
    }

    public BrokerConfig withMaxConnections(int changedLimit) {
        return new BrokerConfig(
                dataDirectory,
                host,
                port,
                flushInterval,
                flushBytes,
                maxFetchBytes,
                protocolLimits,
                logConfig,
                dataWorkers,
                validationWorkers,
                changedLimit,
                maxTasksPerPartition,
                maxValidationTasks,
                maxQueuedRequestBytes,
                maxOutboundPerConnection,
                maxOutboundTotal,
                maxRequestContexts,
                maxFlushedWaiters,
                maxTopics,
                maxPartitions,
                shutdownTimeout);
    }

    private BrokerConfig copy(int changedPort, Duration changedInterval) {
        return new BrokerConfig(
                dataDirectory,
                host,
                changedPort,
                changedInterval,
                flushBytes,
                maxFetchBytes,
                protocolLimits,
                logConfig,
                dataWorkers,
                validationWorkers,
                maxConnections,
                maxTasksPerPartition,
                maxValidationTasks,
                maxQueuedRequestBytes,
                maxOutboundPerConnection,
                maxOutboundTotal,
                maxRequestContexts,
                maxFlushedWaiters,
                maxTopics,
                maxPartitions,
                shutdownTimeout);
    }
}

package vn.huyqt.logbroker.broker.cluster;

import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

import java.util.*;

/**
 * Immutable best-effort diagnostics; this snapshot never grants serving permission.
 *
 * <p>Fields are sampled from independently owned components and need not describe one atomic
 * instant. Offsets are exclusive metadata boundaries. The session may be {@code null} before
 * registration, and heartbeat age is {@code -1} until a valid heartbeat reply is accepted.
 * Collection components are defensively copied; partition failures contain diagnostic reasons.
 */
public record BrokerStatus(
        BrokerIdentityStore.Identity identity,
        Session session,
        BrokerLifecycle.State lifecycle,
        int controllerHint,
        long heartbeatAgeMillis,
        long appliedOffset,
        long durableOffset,
        long snapshotEnd,
        UUID observerGeneration,
        Map<String, Integer> partitionCounts,
        Map<String, Long> budgetUsage,
        Map<TopicPartition, String> partitionFailures) {
    public BrokerStatus {
        partitionCounts = Map.copyOf(partitionCounts);
        budgetUsage = Map.copyOf(budgetUsage);
        partitionFailures = Map.copyOf(partitionFailures);
    }
}

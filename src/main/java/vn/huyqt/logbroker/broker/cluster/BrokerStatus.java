package vn.huyqt.logbroker.broker.cluster;

import java.util.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

/** Immutable best-effort diagnostic snapshot; it never grants serving permission. */
public record BrokerStatus(BrokerIdentityStore.Identity identity, Session session,
        BrokerLifecycle.State lifecycle, int controllerHint, long heartbeatAgeMillis,
        long appliedOffset, long durableOffset, long snapshotEnd, UUID observerGeneration,
        Map<String, Integer> partitionCounts, Map<String, Long> budgetUsage,
        Map<TopicPartition, String> partitionFailures) {
    public BrokerStatus {
        partitionCounts = Map.copyOf(partitionCounts);
        budgetUsage = Map.copyOf(budgetUsage);
        partitionFailures = Map.copyOf(partitionFailures);
    }
}

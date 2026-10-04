package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

class ControllerConfigTest {
    @Test
    void brokerSessionTimeoutMustExceedHeartbeatRpcDeadline() {
        assertEquals(
                java.time.Duration.ofSeconds(10),
                ControllerConfig.builder(identity()).build().brokerSessionTimeout());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ControllerConfig.builder(identity())
                                .brokerSessionTimeout(java.time.Duration.ofSeconds(2))
                                .build());
    }

    @Test
    void clusterBatchMustContainLargestSessionLifecycleChange() {
        var old = identity();
        var v2 = new ClusterIdentity(old.clusterId(), old.nodeId(), old.voters(), (short) 2);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ControllerConfig.builder(v2)
                                .logConfig(
                                        new vn.huyqt.logbroker.storage.LogConfig(
                                                1024 * 1024, 65536, 4096))
                                .build());
        assertDoesNotThrow(() -> ControllerConfig.builder(v2).build());
    }

    @Test
    void eventQueueMustLeaveGeneralAdmissionBeyondReservedSlots() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ControllerConfig.builder(identity())
                                .eventQueueCapacity(768)
                                .diskQueueCapacity(256)
                                .build());
    }

    static ClusterIdentity identity() {
        return new ClusterIdentity(
                new UUID(0, 1),
                0,
                List.of(
                        new ClusterIdentity.Voter(0, "localhost", 19090),
                        new ClusterIdentity.Voter(1, "localhost", 19091),
                        new ClusterIdentity.Voter(2, "localhost", 19092)));
    }

    @Test
    void rejectsQuorumThatCannotContainItsLocalVoter() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ClusterIdentity(new UUID(0, 1), 9, identity().voters()));
    }

    @Test
    void rejectsDuplicateEndpointsAndDuplicateVoters() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ClusterIdentity(
                                new UUID(0, 1),
                                0,
                                List.of(
                                        identity().voters().getFirst(),
                                        identity().voters().getFirst(),
                                        identity().voters().getLast())));
    }

    @Test
    void fetchBudgetMustContainACompleteBatch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ControllerConfig.builder(identity()).fetchMaxBytes(500).build());
    }

    @Test
    void snapshotCapacityMustContainLargestCatalog() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ControllerConfig.builder(identity()).snapshotMaxBytes(100).build());
    }

    @Test
    void snapshotCapacityIncludesClusterAssignmentsAndSupportsConfiguredCounts() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ControllerConfig.builder(identity()).snapshotMaxBytes(65536).build());
        var config =
                ControllerConfig.builder(identity()).maxTopics(1024).maxPartitions(8192).build();
        assertEquals(8192, config.metadataLimits().maxPartitions());
        assertTrue(
                config.metadataLimits().maxEncodedImageBytes() + 102 <= config.snapshotMaxBytes());
    }

    @Test
    void canonicalMembershipHashIndependentOfLocalNodeAndOrdering() {
        var first = identity();
        var second = new ClusterIdentity(first.clusterId(), 2, first.voters().reversed());
        assertArrayEquals(first.voterHash(), second.voterHash());
        assertNotEquals(first.nodeId(), second.nodeId());
    }
}

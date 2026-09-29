package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ControllerConfigTest {
    @Test void eventQueueMustLeaveGeneralAdmissionBeyondReservedSlots() {
        assertThrows(IllegalArgumentException.class,()->ControllerConfig.builder(identity()).eventQueueCapacity(768).diskQueueCapacity(256).build());
    }
    static ClusterIdentity identity() {
        return new ClusterIdentity(new UUID(0, 1), 0, List.of(
            new ClusterIdentity.Voter(0, "localhost", 19090),
            new ClusterIdentity.Voter(1, "localhost", 19091),
            new ClusterIdentity.Voter(2, "localhost", 19092)));
    }
    @Test void rejectsQuorumThatCannotContainItsLocalVoter() {
        assertThrows(IllegalArgumentException.class, () ->
            new ClusterIdentity(new UUID(0, 1), 9, identity().voters()));
    }
    @Test void rejectsDuplicateEndpointsAndDuplicateVoters() {
        assertThrows(IllegalArgumentException.class, () -> new ClusterIdentity(new UUID(0, 1), 0,
            List.of(identity().voters().getFirst(), identity().voters().getFirst(), identity().voters().getLast())));
    }
    @Test void fetchBudgetMustContainACompleteBatch() {
        assertThrows(IllegalArgumentException.class, () -> ControllerConfig.builder(identity())
            .fetchMaxBytes(500).build());
    }
    @Test void snapshotCapacityMustContainLargestCatalog() {
        assertThrows(IllegalArgumentException.class, () -> ControllerConfig.builder(identity())
            .snapshotMaxBytes(100).build());
    }
    @Test void canonicalMembershipHashIndependentOfLocalNodeAndOrdering() {
        var first = identity();
        var second = new ClusterIdentity(first.clusterId(), 2, first.voters().reversed());
        assertArrayEquals(first.voterHash(), second.voterHash());
        assertNotEquals(first.nodeId(), second.nodeId());
    }
}

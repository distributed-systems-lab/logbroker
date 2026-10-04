package vn.huyqt.logbroker.controller.consensus;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.log.*;

import java.util.*;

class CommitRuleTest {
    @Test
    void majorityRequiresMatchingEpochAndCurrentEpochBoundary() {
        var index =
                new EpochIndex(
                        0,
                        0,
                        List.of(
                                new QuorumBatch(0, List.of(new QuorumEntry.ReadBarrier(1))),
                                new QuorumBatch(1, List.of(new QuorumEntry.LeaderChange(2, 0)))));
        var tracker = new ReplicationTracker();
        tracker.reset(2);
        tracker.confirm(1, 1, 1, index);
        assertEquals(0, tracker.committable(2, 2, index));
        tracker.confirm(1, 2, 1, index);
        assertEquals(0, tracker.committable(2, 2, index));
        tracker.confirm(1, 2, 2, index);
        assertEquals(0, tracker.committable(1, 2, index));
        assertEquals(2, tracker.committable(2, 2, index));
        tracker.reset(3);
        assertEquals(0, tracker.committable(2, 3, index));
    }
}

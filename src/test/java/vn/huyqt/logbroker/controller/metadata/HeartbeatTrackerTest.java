package vn.huyqt.logbroker.controller.metadata;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class HeartbeatTrackerTest {
    @Test
    void duplicateCannotExtendSession() {
        var t = new HeartbeatTracker(10);
        var s = new ClusterRecords.Session(1, new UUID(0, 1), new UUID(0, 2), 4);
        t.leaderStarted(3, 0);
        assertTrue(t.record(s, 1, 0));
        assertFalse(t.record(s, 1, 9));
        assertTrue(t.expired(10).contains(1));
        assertFalse(t.eligible(1, 10));
    }

    @Test
    void leadershipResetsSequencesAndRequiresFreshContact() {
        var t = new HeartbeatTracker(10);
        var s = new ClusterRecords.Session(1, new UUID(0, 1), new UUID(0, 2), 4);
        t.leaderStarted(3, 0);
        t.observe(s);
        assertFalse(t.eligible(1, 0));
        assertTrue(t.expired(9).isEmpty());
        assertTrue(t.expired(10).contains(1));
        assertTrue(t.record(s, 100, 10));
        t.leaderStarted(4, 11);
        t.observe(s);
        assertFalse(t.eligible(1, 11));
        assertTrue(t.record(s, 1, 12));
        assertTrue(t.eligible(1, 12));
        assertFalse(t.record(s, 0, 19));
    }

    @Test
    void differentSessionCannotExtendTrackedIncarnation() {
        var t = new HeartbeatTracker(10);
        var s = new ClusterRecords.Session(1, new UUID(0, 1), new UUID(0, 2), 4);
        t.leaderStarted(3, 0);
        t.observe(s);
        assertTrue(t.record(s, 1, 0));
        assertFalse(
                t.record(new ClusterRecords.Session(1, s.storageId(), new UUID(0, 3), 5), 2, 9));
        assertTrue(t.expired(10).contains(1));
    }
}

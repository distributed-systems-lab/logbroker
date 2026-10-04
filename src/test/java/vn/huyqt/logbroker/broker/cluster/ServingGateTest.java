package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;

import java.util.UUID;

class ServingGateTest {
    static Session session(long epoch) {
        return new Session(1, new UUID(0, 1), new UUID(0, 2), epoch);
    }

    @Test
    void oldFenceCannotUndoNewerUnfence() {
        var gate = new ServingGate(session(4));
        assertFalse(gate.canServe());
        gate.apply(10, 10, false);
        gate.reject(8);
        assertTrue(gate.canServe());
        gate.reject(12);
        assertFalse(gate.canServe());
        gate.apply(12, 10, false);
        assertFalse(gate.canServe());
        gate.apply(14, 14, false);
        assertTrue(gate.canServe());
    }

    @Test
    void unobservedDecisionCannotGrantPermission() {
        var gate = new ServingGate(session(4));
        gate.apply(8, 10, false);
        assertFalse(gate.canServe());
        gate.apply(10, 10, false);
        assertTrue(gate.canServe());
        gate.apply(9, 9, true);
        assertTrue(gate.canServe());
    }

    @Test
    void closeIsPermanentAndReplacementInvalidatesPermission() {
        var gate = new ServingGate(session(4));
        gate.apply(10, 10, false);
        gate.bind(session(11));
        assertFalse(gate.canServe());
        gate.apply(10, 10, false);
        assertFalse(gate.canServe());
        gate.apply(12, 12, false);
        assertTrue(gate.canServe());
        gate.close();
        gate.apply(14, 14, false);
        assertFalse(gate.canServe());
        assertThrows(IllegalStateException.class, () -> gate.bind(session(15)));
    }
}

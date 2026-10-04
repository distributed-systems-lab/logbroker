package vn.huyqt.logbroker.controller.metadata;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.log.*;

class ObserverReadServiceTest {
    @Test
    void observerCannotSeeUncommittedTail() {
        var a = new QuorumBatch(0, List.of(new QuorumEntry.ReadBarrier(1)));
        var b = new QuorumBatch(1, List.of(new QuorumEntry.ReadBarrier(1)));
        assertEquals(List.of(a), ObserverReadService.committedPrefix(List.of(a, b), 1));
    }

    @Test
    void atomicBatchIsNeverTrimmedAndGapsAreRejected() {
        var a =
                new QuorumBatch(
                        0, List.of(new QuorumEntry.ReadBarrier(1), new QuorumEntry.ReadBarrier(1)));
        assertTrue(ObserverReadService.committedPrefix(List.of(a), 1).isEmpty());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ObserverReadService.committedPrefix(
                                List.of(
                                        a,
                                        new QuorumBatch(
                                                3, List.of(new QuorumEntry.ReadBarrier(1)))),
                                4));
    }
}

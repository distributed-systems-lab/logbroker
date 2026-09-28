package vn.huyqt.logbroker.controller.log;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import org.junit.jupiter.api.Test;

class EpochIndexTest {
    @Test void reconciliationReturnsGreatestCommonEpochBoundaryRatherThanLargestOffset() {
        var index=new EpochIndex(0,0,List.of(
            new QuorumBatch(0,List.of(new QuorumEntry.ReadBarrier(1))),
            new QuorumBatch(1,List.of(new QuorumEntry.ReadBarrier(3)))));
        assertEquals(new EpochIndex.LogPosition(1,1),index.commonPrefix(2,2));
        assertEquals(new EpochIndex.LogPosition(1,1),index.commonPrefix(1,1));
        assertEquals(new EpochIndex.LogPosition(2,3),index.positionAt(2));
        assertEquals(1,index.endOfEpoch(1));
    }
    @Test void compactedPrefixCarriesSnapshotEpochAndOnlyBatchBoundariesAreAllowed() {
        var index=new EpochIndex(10,4,List.of(new QuorumBatch(10,List.of(
            new QuorumEntry.ReadBarrier(5),new QuorumEntry.ReadBarrier(5)))));
        assertEquals(new EpochIndex.LogPosition(10,4),index.positionAt(10));
        assertThrows(IllegalArgumentException.class,()->index.positionAt(11));
        assertThrows(IllegalArgumentException.class,()->index.commonPrefix(8,3));
    }
}

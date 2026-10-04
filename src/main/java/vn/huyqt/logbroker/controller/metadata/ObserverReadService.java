package vn.huyqt.logbroker.controller.metadata;

import vn.huyqt.logbroker.controller.log.QuorumBatch;

import java.util.*;

/** Whole, contiguous committed batches; observers never reconcile by truncating applied history. */
public final class ObserverReadService {
    private ObserverReadService() {}

    public static List<QuorumBatch> committedPrefix(List<QuorumBatch> batches, long commitOffset) {
        if (commitOffset < 0) throw new IllegalArgumentException("Negative committed offset");
        long end = batches.isEmpty() ? 0 : batches.getFirst().baseOffset();
        for (var batch : batches) {
            if (batch.baseOffset() != end)
                throw new IllegalArgumentException("Noncontiguous observer read");
            end = batch.nextOffset();
        }
        return batches.stream().takeWhile(b -> b.nextOffset() <= commitOffset).toList();
    }
}

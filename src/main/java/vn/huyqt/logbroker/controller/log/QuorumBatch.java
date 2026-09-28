package vn.huyqt.logbroker.controller.log;

import java.util.List;

public record QuorumBatch(long baseOffset, List<QuorumEntry> entries) {
    public QuorumBatch {
        entries = List.copyOf(entries);
        if (baseOffset < 0 || entries.isEmpty()) throw new IllegalArgumentException("Invalid batch");
        try { Math.addExact(baseOffset, entries.size()); }
        catch (ArithmeticException e) { throw new IllegalArgumentException("Offset overflow", e); }
        long epoch = entries.getFirst().epoch();
        if (entries.stream().anyMatch(e -> e.epoch() != epoch)) throw new IllegalArgumentException("Mixed batch epoch");
    }
    public long nextOffset() { return Math.addExact(baseOffset, entries.size()); }
}

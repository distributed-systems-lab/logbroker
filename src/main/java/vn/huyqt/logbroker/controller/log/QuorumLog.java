package vn.huyqt.logbroker.controller.log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;
import vn.huyqt.logbroker.storage.*;

/** Metadata payload adapter; storage remains unaware of elections and quorum progress. */
public final class QuorumLog implements AutoCloseable {
    private final PartitionLog log; private final LogConfig config;
    private long originOffset,originEpoch; private EpochIndex index;
    private QuorumLog(PartitionLog log,LogConfig config,long originOffset,long originEpoch) throws IOException {
        this.log=log; this.config=config; this.originOffset=originOffset;this.originEpoch=originEpoch; rebuild();
    }
    public static QuorumLog open(Path path,LogConfig config,long start,long minimumEnd,
            boolean create,DurableFiles files) throws IOException {
        return open(path,config,start,minimumEnd,0,create,files);
    }
    public static QuorumLog open(Path path,LogConfig config,long start,long minimumEnd,long originEpoch,
            boolean create,DurableFiles files) throws IOException {
        return open(path,config,start,minimumEnd,start,originEpoch,create,files);
    }
    public static QuorumLog open(Path path,LogConfig config,long physicalStart,long minimumEnd,long logicalStart,long originEpoch,
            boolean create,DurableFiles files) throws IOException {
        PartitionLog log=PartitionLog.open(path,config,new LogOpenOptions(physicalStart,minimumEnd,create,files::syncDirectory));
        try { return new QuorumLog(log,config,logicalStart,originEpoch); }
        catch(IOException|RuntimeException e) { log.close(); throw e; }
    }
    public QuorumBatch append(long epoch,List<QuorumEntry> entries) throws IOException {
        var batch=new QuorumBatch(log.logEndOffset(),entries);
        if(batch.entries().getFirst().epoch()!=epoch)throw new IllegalArgumentException("Wrong append epoch");
        appendReplica(batch); return batch;
    }
    public void appendReplica(QuorumBatch batch) throws IOException {
        if(batch.baseOffset()!=end()||batch.entries().getFirst().epoch()<index.positionAt(end()).lastEpoch())
            throw new IllegalArgumentException("Replica base or epoch mismatch");
        var records=new ArrayList<LogRecord>(); long wireBytes=16;
        for(var entry:batch.entries()) {
            byte[] payload=QuorumEntryCodec.encode(entry); wireBytes=Math.addExact(wireBytes,4L+payload.length);
            records.add(new LogRecord(0,null,payload,List.of()));
        }
        if(wireBytes>config.maxBatchBytes()||30L+RecordPayloadCodec.encodedSize(records)>config.maxBatchBytes())
            throw new IllegalArgumentException("Metadata batch too large");
        log.append(records); rebuild();
    }
    public List<QuorumBatch> read(long offset,int budget) throws IOException {
        if(offset<originOffset)throw new IOException("Offset precedes quorum recovery base");
        return readPhysical(offset,budget);
    }
    private List<QuorumBatch> readPhysical(long offset,int budget) throws IOException {
        var result=new ArrayList<QuorumBatch>();
        for(var batch:log.read(offset,budget)) {
            var entries=new ArrayList<QuorumEntry>();
            for(var record:batch.records()) {
                if(record.timestamp()!=0||record.key()!=null||!record.headers().isEmpty())throw new IOException("Invalid quorum record");
                entries.add(QuorumEntryCodec.decode(record.value()));
            }
            try { result.add(new QuorumBatch(batch.baseOffset(),entries)); }
            catch(IllegalArgumentException e) { throw new IOException("Invalid quorum batch",e); }
        }
        return List.copyOf(result);
    }
    private void rebuild() throws IOException {
        var batches=new ArrayList<QuorumBatch>(); long offset=log.logStartOffset();
        while(offset<log.logEndOffset()) {
            var next=readPhysical(offset,config.maxBatchBytes()); if(next.isEmpty())throw new IOException("Quorum replay gap");
            batches.addAll(next); offset=next.getLast().nextOffset();
        }
        if(originOffset<log.logStartOffset()||originOffset>log.logEndOffset()||originOffset!=log.logStartOffset()&&batches.stream().noneMatch(b->b.nextOffset()==originOffset))throw new IOException("Invalid logical quorum origin");
        try { index=new EpochIndex(originOffset,originEpoch,batches.stream().filter(b->b.baseOffset()>=originOffset).toList()); }
        catch(IllegalArgumentException e) { throw new IOException("Invalid quorum epoch sequence",e); }
    }
    public void truncate(long end,long knownCommit) throws IOException {
        if(end<knownCommit)throw new IllegalArgumentException("Truncation crosses commit");
        index.positionAt(end); log.truncateTo(end); rebuild();
    }
    public long flush() throws IOException { return log.flush(); }
    public long end() { return log.logEndOffset(); }
    public long durableEnd() { return log.durableEndOffset(); }
    public long start() { return originOffset; }
    public void advanceStart(long start,long epoch)throws IOException {
        if(start<originOffset||index.positionAt(start).lastEpoch()!=epoch)throw new IllegalArgumentException("Invalid recovery base");
        originOffset=start;originEpoch=epoch;rebuild();
    }
    public void refresh()throws IOException {rebuild();}
    public EpochIndex epochs() { return index; }
    public PartitionLog storage() { return log; }
    @Override public void close() throws IOException { log.close(); }
}

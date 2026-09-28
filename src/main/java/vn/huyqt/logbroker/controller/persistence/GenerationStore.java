package vn.huyqt.logbroker.controller.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.storage.*;

/** Journal references decide the active generation; directory discovery cannot invent committed state. */
public final class GenerationStore implements AutoCloseable {
    private final QuorumStateStore state; private final DurableFiles files; private final LogConfig config;
    private UUID generation; private long start,committed; private QuorumLog log;
    private GenerationStore(QuorumStateStore state,DurableFiles files,LogConfig config) {
        this.state=state;this.files=files;this.config=config;
    }
    public static GenerationStore open(QuorumStateStore state,DurableFiles files,LogConfig config)throws IOException {
        var store=new GenerationStore(state,files,config); store.recover(); return store;
    }
    private void recover()throws IOException {
        Long truncate=null; long truncateSequence=0; Long prefix=null; long prefixSequence=0;
        for(var frame:state.journal().frames()) {
            var in=ByteBuffer.wrap(frame.payload());
            try {
                switch(frame.type()) {
                    case StateJournal.GENERATION -> {
                        UUID next=new UUID(in.getLong(),in.getLong()); long nextStart=in.getLong(); byte snapshot=in.get();
                        if(nextStart<0||snapshot!=0||in.hasRemaining())throw new IOException("Invalid generation record");
                        if(!next.equals(generation)) { committed=nextStart;truncate=null;prefix=null; }
                        generation=next;start=nextStart;
                    }
                    case StateJournal.COMMIT -> {
                        UUID id=new UUID(in.getLong(),in.getLong());long end=in.getLong();
                        if(in.hasRemaining())throw new IOException("Invalid commit length");
                        if(id.equals(generation)) { if(end<committed)throw new IOException("Commit regression");committed=end; }
                    }
                    case StateJournal.TRUNCATE_INTENT -> {
                        UUID id=new UUID(in.getLong(),in.getLong());long end=in.getLong();
                        if(in.hasRemaining())throw new IOException("Invalid truncate intent");
                        if(id.equals(generation)) { truncate=end;truncateSequence=frame.sequence(); }
                    }
                    case StateJournal.TRUNCATE_DONE -> { if(in.getLong()==truncateSequence)truncate=null; if(in.hasRemaining())throw new IOException("Invalid truncate completion"); }
                    case StateJournal.PREFIX_INTENT -> {
                        UUID id=new UUID(in.getLong(),in.getLong());long value=in.getLong();
                        if(in.hasRemaining())throw new IOException("Invalid prefix intent");
                        if(id.equals(generation)) { prefix=value;start=value;prefixSequence=frame.sequence(); }
                    }
                    case StateJournal.PREFIX_DONE -> { if(in.getLong()==prefixSequence)prefix=null; if(in.hasRemaining())throw new IOException("Invalid prefix completion"); }
                    default -> { }
                }
            }catch(java.nio.BufferUnderflowException e){throw new IOException("Truncated generation journal payload",e);}
        }
        if(generation==null)throw new IOException("No published generation");
        Path directory=directory();
        if(prefix!=null) {
            if(prefix>committed)throw new IOException("Prefix intent exceeds commit");
            try(var paths=Files.list(directory)) {
                for(var path:paths.toList()) {
                    String name=path.getFileName().toString();
                    if(name.matches("[0-9]{20}\\.(log|index)")&&Long.parseLong(name.substring(0,20))<prefix)Files.delete(path);
                }
            }
            files.syncDirectory(directory);
            state.journal().append(StateJournal.PREFIX_DONE,ByteBuffer.allocate(8).putLong(prefixSequence).array());
        }
        if(truncate!=null) {
            LogIntentRecovery.truncate(directory,config,start,committed,truncate,files::syncDirectory);
            state.journal().append(StateJournal.TRUNCATE_DONE,ByteBuffer.allocate(8).putLong(truncateSequence).array());
        }
        log=QuorumLog.open(directory,config,start,committed,false,files);
    }
    public void checkpointCommit(long end)throws IOException {
        if(end<committed||end>log.durableEnd())throw new IllegalArgumentException("Invalid committed boundary");
        log.epochs().positionAt(end);
        if(end==committed)return;
        state.journal().append(StateJournal.COMMIT,encodeBoundary(end)); committed=end;
    }
    public void truncate(long end)throws IOException {
        if(end<committed)throw new IllegalArgumentException("Truncate crosses commit");log.epochs().positionAt(end);
        long intent=state.journal().append(StateJournal.TRUNCATE_INTENT,encodeBoundary(end));
        log.truncate(end,committed);
        state.journal().append(StateJournal.TRUNCATE_DONE,ByteBuffer.allocate(8).putLong(intent).array());
    }
    private byte[] encodeBoundary(long end) { return ByteBuffer.allocate(24).putLong(generation.getMostSignificantBits()).putLong(generation.getLeastSignificantBits()).putLong(end).array(); }
    public Path directory() { return state.root().resolve("generations").resolve(generation.toString()).resolve("log"); }
    public QuorumLog log() { return log; }
    public UUID generation() { return generation; }
    public long committedOffset() { return committed; }
    @Override public void close()throws IOException { if(log!=null)log.close(); }
}

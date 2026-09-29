package vn.huyqt.logbroker.controller.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.storage.*;
import vn.huyqt.logbroker.controller.snapshot.*;
import vn.huyqt.logbroker.controller.metadata.*;

/** Journal references decide the active generation; directory discovery cannot invent committed state. */
public final class GenerationStore implements AutoCloseable {
    public static final class InstallCancelled extends IOException {public InstallCancelled(){super("Snapshot install fence expired");}}
    private final QuorumStateStore state; private final DurableFiles files; private final LogConfig config;
    private volatile UUID generation; private long start,committed; private QuorumLog log;
    private SnapshotId baseSnapshot;
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
                        UUID next=new UUID(in.getLong(),in.getLong()); long nextStart=in.getLong();byte present=in.get();
                        if(present!=0&&present!=1)throw new IOException("Invalid generation snapshot flag");
                        SnapshotId snapshot=present==1?SnapshotId.readFrom(in):null;
                        if(nextStart<0||snapshot!=null&&snapshot.endOffset()<nextStart||in.hasRemaining())throw new IOException("Invalid generation record");
                        if(!next.equals(generation)) { committed=snapshot==null?nextStart:snapshot.endOffset();truncate=null;prefix=null; }
                        else if(snapshot!=null) {if(baseSnapshot!=null&&snapshot.endOffset()<baseSnapshot.endOffset())throw new IOException("Recovery base regression");committed=Math.max(committed,snapshot.endOffset());}
                        generation=next;start=nextStart;baseSnapshot=snapshot;
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
        long logicalStart=baseSnapshot==null?start:baseSnapshot.endOffset(),lastEpoch=baseSnapshot==null?0:baseSnapshot.lastEpoch();
        if(baseSnapshot!=null)new SnapshotStore(state.root(),state.identity(),files,state,64*1024*1024).load(baseSnapshot);
        log=QuorumLog.open(directory,config,start,committed,logicalStart,lastEpoch,false,files);
        try {log.epochs().positionAt(committed);}catch(IllegalArgumentException e){log.close();throw new IOException("Commit is not a retained batch boundary",e);}
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
    public void retainPrefix(long olderSnapshotEnd)throws IOException {
        var snapshots=new SnapshotStore(state.root(),state.identity(),files,state,64*1024*1024);
        if(snapshots.retained().size()!=2||snapshots.retained().get(1).endOffset()!=olderSnapshotEnd||olderSnapshotEnd>committed)throw new IllegalArgumentException("Retention requires two committed snapshots");
        var base=snapshots.retained().get(1);log.epochs().positionAt(base.endOffset());
        if(base.endOffset()<log.start()||log.epochs().positionAt(base.endOffset()).lastEpoch()!=base.lastEpoch())throw new IllegalArgumentException("Snapshot epoch mismatch");
        // The new recovery base is durable before any bytes required by the old base disappear.
        state.journal().append(StateJournal.GENERATION,encodeGeneration(generation,start,base));baseSnapshot=base;
        long nextStart=log.storage().prefixStartAfter(olderSnapshotEnd);
        long intent=state.journal().append(StateJournal.PREFIX_INTENT,encodeBoundary(nextStart));
        start=nextStart;log.advanceStart(base.endOffset(),base.lastEpoch());log.storage().deleteSegmentsBefore(olderSnapshotEnd);log.refresh();
        state.journal().append(StateJournal.PREFIX_DONE,ByteBuffer.allocate(8).putLong(intent).array());
    }
    public static byte[] encodeGeneration(UUID id,long start,SnapshotId snapshot) {
        var out=ByteBuffer.allocate(snapshot==null?25:57).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).putLong(start).put((byte)(snapshot==null?0:1));
        if(snapshot!=null)snapshot.writeTo(out);return out.array();
    }
    public static SnapshotId snapshotReference(QuorumStateStore state)throws IOException {
        SnapshotId result=null;
        for(var frame:state.journal().frames())if(frame.type()==StateJournal.GENERATION) {
            try {var in=ByteBuffer.wrap(frame.payload());in.getLong();in.getLong();in.getLong();byte present=in.get();if(present!=0&&present!=1)throw new IOException("Invalid generation snapshot flag");result=present==1?SnapshotId.readFrom(in):null;if(in.hasRemaining())throw new IOException("Invalid generation payload");}
            catch(java.nio.BufferUnderflowException|IllegalArgumentException e){throw new IOException("Invalid generation snapshot reference",e);}
        }
        return result;
    }
    public MetadataImage recoveredImage()throws IOException {
        var metadata=new MetadataStateMachine();
        if(baseSnapshot!=null)metadata.restore(new SnapshotStore(state.root(),state.identity(),files,state,64*1024*1024).load(baseSnapshot));
        while(metadata.image().appliedOffset()<committed) {
            var batches=log.read(metadata.image().appliedOffset(),config.maxBatchBytes());
            if(batches.isEmpty())throw new IOException("Committed replay gap");
            for(var batch:batches){if(batch.nextOffset()>committed)break;metadata.apply(batch);}
        }
        return metadata.image();
    }
    public EpochIndex epochIndex(){return log.epochs();}
    public SnapshotId baseSnapshot(){return baseSnapshot;}
    public void install(SnapshotId id,MetadataImage image)throws IOException {install(id,image,()->true);}
    /** Best-effort cleanup after publication and drain; failure leaves recoverable garbage only. */
    public boolean releaseObsoleteGenerations() {
        Path parent=state.root().resolve("generations").toAbsolutePath().normalize();
        try(var children=Files.list(parent)) {
            for(var child:children.toList()) {
                Path target=child.toAbsolutePath().normalize();String name=target.getFileName().toString();UUID id;
                try{id=UUID.fromString(name);}catch(IllegalArgumentException ignored){continue;}
                if(!id.toString().equals(name)||id.equals(generation)||!target.getParent().equals(parent)||!Files.isDirectory(target,LinkOption.NOFOLLOW_LINKS))continue;
                try(var paths=Files.walk(target)) {
                    for(var path:paths.sorted(Comparator.reverseOrder()).toList()) {
                        if(!path.toAbsolutePath().normalize().startsWith(target))throw new IOException("Generation cleanup escaped root");Files.delete(path);
                    }
                }
            }
            files.syncDirectory(parent);return true;
        }catch(IOException e){return false;}
    }
    public void install(SnapshotId id,MetadataImage image,java.util.function.BooleanSupplier allowed)throws IOException {
        if(image.appliedOffset()!=id.endOffset()||id.endOffset()<committed||id.endOffset()<=log.start())throw new IOException("Snapshot does not advance safe prefix");
        if(log.epochs().positionAt(committed).lastEpoch()>id.lastEpoch())throw new IOException("Snapshot contradicts known committed epoch");
        var snapshots=new SnapshotStore(state.root(),state.identity(),files,state,64*1024*1024);
        if(!image.equals(snapshots.load(id)))throw new IOException("Installed image differs from immutable snapshot");
        UUID next=UUID.randomUUID();Path generationDirectory=state.root().resolve("generations").resolve(next.toString());
        Files.createDirectory(generationDirectory);files.syncDirectory(generationDirectory.getParent());
        Path directory=generationDirectory.resolve("log");QuorumLog replacement=null;
        try {
            replacement=QuorumLog.open(directory,config,id.endOffset(),id.endOffset(),id.lastEpoch(),true,files);
            replacement.flush();files.syncDirectory(generationDirectory);files.syncDirectory(generationDirectory.getParent());
            if(!allowed.getAsBoolean())throw new InstallCancelled();
            log.close();
            if(!allowed.getAsBoolean()) {
                log=QuorumLog.open(directory(),config,start,committed,baseSnapshot==null?start:baseSnapshot.endOffset(),baseSnapshot==null?0:baseSnapshot.lastEpoch(),false,files);
                throw new InstallCancelled();
            }
            state.journal().append(StateJournal.GENERATION,encodeGeneration(next,id.endOffset(),id));
            generation=next;start=id.endOffset();committed=id.endOffset();baseSnapshot=id;log=replacement;replacement=null;
            snapshots.publishInstalled(id);
        } finally {if(replacement!=null)replacement.close();}
    }
    private byte[] encodeBoundary(long end) { return ByteBuffer.allocate(24).putLong(generation.getMostSignificantBits()).putLong(generation.getLeastSignificantBits()).putLong(end).array(); }
    public Path directory() { return state.root().resolve("generations").resolve(generation.toString()).resolve("log"); }
    public QuorumLog log() { return log; }
    public UUID generation() { return generation; }
    public long committedOffset() { return committed; }
    @Override public void close()throws IOException { if(log!=null)log.close(); }
}

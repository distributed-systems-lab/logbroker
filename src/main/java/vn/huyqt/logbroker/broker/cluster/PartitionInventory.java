package vn.huyqt.logbroker.broker.cluster;

import static java.nio.file.StandardOpenOption.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

/** Ordered-worker-owned provisioning ledger. Every published transition has been forced. */
public final class PartitionInventory implements AutoCloseable {
    public enum State { NEW, INTENT, COMPLETE }
    private static final int MAGIC=0x50494e32;
    private static final long CAP=64L*1024*1024;
    private final Path path;
    private final UUID storage;
    private final DurableFiles files;
    private final FileChannel channel;
    private final Map<TopicPartition,State> states=new HashMap<>();
    private long sequence;
    private boolean failed;
    private PartitionInventory(Path path,UUID storage,DurableFiles files,FileChannel channel) {
        this.path=path; this.storage=storage; this.files=files; this.channel=channel;
    }
    public static void format(Path root,UUID storage,DurableFiles files) throws IOException {
        requireStorage(storage); Files.createDirectories(root); files.verifySupport(root);
        byte[] payload=ByteBuffer.allocate(16).putLong(storage.getMostSignificantBits()).putLong(storage.getLeastSignificantBits()).array();
        files.writeNew(root.resolve("partition-inventory.journal"),encode(1,(byte)0,payload)); files.syncDirectory(root);
    }
    /** Opens only a formatted ledger; absence is never interpreted as an empty inventory. */
    public static PartitionInventory open(Path root,UUID storage,DurableFiles files) throws IOException {
        requireStorage(storage); Path path=root.resolve("partition-inventory.journal");
        if(!Files.isRegularFile(path)) throw new IOException("Missing partition inventory");
        var channel=FileChannel.open(path,READ,WRITE); var inventory=new PartitionInventory(path,storage,files,channel);
        try { inventory.recover(); return inventory; }
        catch(IOException | RuntimeException e) { channel.close(); throw e; }
    }
    private static void requireStorage(UUID storage) {
        if(storage==null || storage.equals(new UUID(0,0))) throw new IllegalArgumentException("Invalid inventory storage identity");
    }
    private static byte[] encode(long sequence,byte type,byte[] payload) {
        byte[] bytes=new byte[23+payload.length]; var out=ByteBuffer.wrap(bytes);
        out.putInt(MAGIC).putShort((short)2).putInt(bytes.length).putLong(sequence).put(type).put(payload)
            .putInt(StateJournal.crc(bytes,0,bytes.length-4)); return bytes;
    }
    private byte[] read(long position,int length) throws IOException {
        var buffer=ByteBuffer.allocate(length);
        while(buffer.hasRemaining()) { int count=channel.read(buffer,position); if(count<=0) throw new IOException("Inventory read stalled"); position+=count; }
        return buffer.array();
    }
    private void recover() throws IOException {
        long size=channel.size(),position=0; if(size>CAP) throw new IOException("Inventory capacity exceeded");
        while(position<size) {
            int available=(int)Math.min(19,size-position); byte[] header=read(position,available);
            byte[] expected=ByteBuffer.allocate(6).putInt(MAGIC).putShort((short)2).array();
            for(int i=0;i<Math.min(6,available);i++) if(header[i]!=expected[i]) throw new IOException("Invalid inventory header");
            if(available<10) {
                byte[] lengthPrefix=ByteBuffer.allocate(4).putInt(sequence==0 ? 39 : 43).array();
                for(int i=6;i<available;i++) if(header[i]!=lengthPrefix[i-6]) throw new IOException("Invalid partial inventory length");
                repairTail(position); break;
            }
            var in=ByteBuffer.wrap(header); in.position(6); int length=in.getInt();
            if(length!=39 && length!=43) throw new IOException("Invalid inventory frame length");
            byte[] nextSequence=ByteBuffer.allocate(8).putLong(sequence+1).array();
            for(int i=10;i<Math.min(18,available);i++) if(header[i]!=nextSequence[i-10]) throw new IOException("Invalid partial inventory sequence");
            if(available>=18 && in.getLong()!=sequence+1) throw new IOException("Inventory sequence gap");
            if(available==19) {
                byte type=header[18];
                if(type<0 || type>2 || sequence==0 && (type!=0 || length!=39)
                    || sequence>0 && (type==0 || length!=43)) throw new IOException("Invalid inventory frame type");
            }
            if(size-position<length) { repairTail(position); break; }
            byte[] frame=read(position,length); var body=ByteBuffer.wrap(frame);
            if(body.getInt(length-4)!=StateJournal.crc(frame,0,length-4)) throw new IOException("Inventory checksum mismatch");
            byte type=frame[18]; body.position(19);
            if(type==0) {
                if(!new UUID(body.getLong(),body.getLong()).equals(storage)) throw new IOException("Inventory storage identity mismatch");
            } else {
                TopicPartition partition;
                try { partition=new TopicPartition(new UUID(body.getLong(),body.getLong()),body.getInt()); validate(partition); }
                catch(IllegalArgumentException e) { throw new IOException("Invalid inventory partition",e); }
                State previous=state(partition);
                if(type==1 && previous!=State.NEW || type==2 && previous!=State.INTENT) throw new IOException("Invalid inventory transition");
                states.put(partition,type==1 ? State.INTENT : State.COMPLETE);
            }
            sequence++; position+=length;
        }
        if(sequence==0) throw new IOException("Missing inventory INIT");
    }
    private void repairTail(long position) throws IOException {
        if(sequence==0) throw new IOException("Incomplete inventory INIT");
        channel.truncate(position); files.forceFile(path);
    }
    private static void validate(TopicPartition partition) {
        Objects.requireNonNull(partition);
        if(partition.topicId().equals(new UUID(0,0)) || partition.partition()<0) throw new IllegalArgumentException("Invalid inventory partition");
    }
    public State state(TopicPartition partition) { return states.getOrDefault(partition,State.NEW); }
    public void begin(TopicPartition partition) throws IOException {
        healthy(); validate(partition); if(state(partition)!=State.NEW) return; append(partition,(byte)1,State.INTENT);
    }
    public void complete(TopicPartition partition) throws IOException {
        healthy(); validate(partition); if(state(partition)==State.COMPLETE) return;
        if(state(partition)!=State.INTENT) throw new IllegalStateException("COMPLETE requires a forced INTENT");
        append(partition,(byte)2,State.COMPLETE);
    }
    private void healthy() throws IOException { if(failed || !channel.isOpen()) throw new IOException("Inventory failed or closed"); }
    private void append(TopicPartition partition,byte type,State next) throws IOException {
        byte[] payload=ByteBuffer.allocate(20).putLong(partition.topicId().getMostSignificantBits())
            .putLong(partition.topicId().getLeastSignificantBits()).putInt(partition.partition()).array();
        byte[] bytes=encode(sequence+1,type,payload);
        try {
            long position=channel.size(); if(position>CAP-bytes.length) throw new IOException("Inventory capacity exhausted");
            var buffer=ByteBuffer.wrap(bytes);
            while(buffer.hasRemaining()) { int count=channel.write(buffer,position); if(count<=0) throw new IOException("Inventory write stalled"); position+=count; }
            files.forceFile(path); sequence++; states.put(partition,next);
        } catch(IOException e) { failed=true; throw e; }
    }
    @Override public void close() throws IOException { channel.close(); }
}

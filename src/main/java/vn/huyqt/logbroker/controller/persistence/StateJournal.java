package vn.huyqt.logbroker.controller.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;
import static java.nio.file.StandardOpenOption.*;

/** Append-and-force journal; complete corrupted frames are never treated as torn tails. */
public final class StateJournal implements AutoCloseable {
    public static final short HARD_STATE=1, GENERATION=2, COMMIT=3, TRUNCATE_INTENT=4,
        TRUNCATE_DONE=5, SNAPSHOT_SET=6, PREFIX_INTENT=7, PREFIX_DONE=8;
    private static final int MAGIC=0x514a4e31, MIN=24, MAX=65536;
    private static final long CAP=64L * 1024 * 1024;
    public record Frame(long sequence, short type, byte[] payload) {
        public Frame { payload = payload.clone(); }
        @Override public byte[] payload() { return payload.clone(); }
    }
    private final Path path; private final DurableFiles files; private final FileChannel channel;
    private final List<Frame> frames = new ArrayList<>();
    private boolean failed;
    private StateJournal(Path path, DurableFiles files, FileChannel channel) {
        this.path=path; this.files=files; this.channel=channel;
    }
    public static StateJournal open(Path path, DurableFiles files) throws IOException {
        boolean created = !Files.exists(path);
        FileChannel channel = FileChannel.open(path, CREATE, READ, WRITE);
        var journal = new StateJournal(path, files, channel);
        try {
            if (created) { files.forceFile(path); files.syncDirectory(path.getParent()); }
            journal.recover(); return journal;
        } catch (IOException | RuntimeException e) { channel.close(); throw e; }
    }
    public List<Frame> frames() { return List.copyOf(frames); }
    public long append(short type, byte[] payload) throws IOException {
        if (failed || !channel.isOpen()) throw new IOException("Journal is failed or closed");
        if (type < 1 || type > 8 || payload.length > MAX-MIN) throw new IllegalArgumentException("Invalid journal frame");
        long sequence = frames.size() + 1L;
        byte[] bytes = new byte[MIN+payload.length]; var out=ByteBuffer.wrap(bytes);
        out.putInt(MAGIC).putShort((short)1).putInt(bytes.length).putLong(sequence).putShort(type).put(payload);
        out.putInt(crc(bytes, 0, bytes.length-4));
        try {
            long position=channel.size();
            if (position > CAP-bytes.length) throw new IOException("State journal capacity exhausted");
            var buffer=ByteBuffer.wrap(bytes);
            while(buffer.hasRemaining()) {
                int n=channel.write(buffer,position); if(n<=0) throw new IOException("No journal write progress"); position+=n;
            }
            files.forceFile(path);
            frames.add(new Frame(sequence,type,payload)); return sequence;
        } catch(IOException e) { failed=true; throw e; }
    }
    private void recover() throws IOException {
        long position=0,size=channel.size();
        if(size>CAP) throw new IOException("Journal exceeds capacity");
        while(position<size) {
            int available=(int)Math.min(20,size-position);
            byte[] header=read(position,available); var input=ByteBuffer.wrap(header);
            byte[] expectedMagic=ByteBuffer.allocate(4).putInt(MAGIC).array();
            for(int i=0;i<Math.min(4,available);i++)
                if(header[i]!=expectedMagic[i]) throw new IOException("Invalid journal magic");
            if(available>=5 && header[4]!=0 || available>=6 && header[5]!=1) throw new IOException("Unsupported journal version");
            int length=0;
            if(available>=10) {
                length=input.getInt(6);
                if(length<MIN||length>MAX) throw new IOException("Invalid journal length");
            }
            if(available>=18 && input.getLong(10)!=frames.size()+1L) throw new IOException("Journal sequence gap");
            if(available>=20 && (input.getShort(18)<1||input.getShort(18)>8)) throw new IOException("Invalid journal type");
            if(available<20||length>size-position) {
                channel.truncate(position); files.forceFile(path); break;
            }
            byte[] bytes=read(position,length); var in=ByteBuffer.wrap(bytes);
            if(in.getInt(length-4)!=crc(bytes,0,length-4)) throw new IOException("Journal checksum mismatch");
            frames.add(new Frame(in.getLong(10),in.getShort(18),Arrays.copyOfRange(bytes,20,length-4)));
            position+=length;
        }
    }
    private byte[] read(long position,int count) throws IOException {
        var buffer=ByteBuffer.allocate(count);
        while(buffer.hasRemaining()) { int n=channel.read(buffer,position); if(n<=0) throw new IOException("Unexpected journal EOF"); position+=n; }
        return buffer.array();
    }
    public static int crc(byte[] bytes,int offset,int length) {
        var crc=new CRC32C(); crc.update(bytes,offset,length); return (int)crc.getValue();
    }
    @Override public void close() throws IOException { channel.close(); }
}

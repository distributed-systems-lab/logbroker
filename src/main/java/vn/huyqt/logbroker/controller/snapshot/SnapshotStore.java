package vn.huyqt.logbroker.controller.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import static java.nio.file.StandardOpenOption.READ;

/** Immutable snapshots are visible only through a forced journal publication. */
public final class SnapshotStore {
    private final Path directory; private final ClusterIdentity identity; private final DurableFiles files;
    private final QuorumStateStore state; private final int maxBytes;
    private List<SnapshotId> retained=List.of(); private final Map<SnapshotId,Integer> pins=new HashMap<>();
    public SnapshotStore(Path root,ClusterIdentity identity,DurableFiles files,QuorumStateStore state,int maxBytes)throws IOException {
        directory=root.resolve("snapshots");this.identity=identity;this.files=files;this.state=state;this.maxBytes=maxBytes;
        if(maxBytes<114||maxBytes>64*1024*1024)throw new IllegalArgumentException("Invalid snapshot limit");
        if(!Files.isDirectory(directory))throw new IOException("Published snapshot directory missing");
        for(var frame:state.journal().frames())if(frame.type()==StateJournal.SNAPSHOT_SET) {
            var in=ByteBuffer.wrap(frame.payload());
            try {
                int count=in.getInt();if(count<0||count>2||in.remaining()!=count*32)throw new IOException("Invalid snapshot set");
                var ids=new ArrayList<SnapshotId>();for(int i=0;i<count;i++)ids.add(SnapshotId.readFrom(in));
                if(ids.stream().map(SnapshotId::contentId).distinct().count()!=count)throw new IOException("Duplicate snapshot IDs");
                retained=List.copyOf(ids);
            }catch(java.nio.BufferUnderflowException|IllegalArgumentException e){throw new IOException("Invalid snapshot journal",e);}
        }
        for(var id:retained)load(id);
    }
    public SnapshotId create(MetadataImage image,long lastEpoch)throws IOException {
        var id=new SnapshotId(image.appliedOffset(),lastEpoch,UUID.randomUUID());
        byte[] bytes=encode(id,image);Path temporary=directory.resolve(id.contentId()+".partial");
        files.writeNew(temporary,bytes);
        Files.move(temporary,path(id),StandardCopyOption.ATOMIC_MOVE);
        files.syncDirectory(directory);
        publish(id);return id;
    }
    private void publish(SnapshotId id)throws IOException {
        var next=new ArrayList<SnapshotId>();next.add(id);
        for(var existing:retained)if(!existing.equals(id)&&next.size()<2)next.add(existing);
        ByteBuffer payload=ByteBuffer.allocate(4+next.size()*32).putInt(next.size());
        for(var snapshot:next)snapshot.writeTo(payload);
        state.journal().append(StateJournal.SNAPSHOT_SET,payload.array());retained=List.copyOf(next);
    }
    public byte[] encode(SnapshotId id,MetadataImage image)throws IOException {
        if(image.appliedOffset()!=id.endOffset())throw new IOException("Snapshot image boundary mismatch");
        byte[] payload=MetadataImageCodec.encode(image);long length=102L+payload.length;
        if(length>maxBytes)throw new IOException("Snapshot exceeds configured limit");
        byte[] bytes=new byte[(int)length];var out=ByteBuffer.wrap(bytes);
        out.putInt(0x51534e31).putShort((short)1).putLong(length)
            .putLong(identity.clusterId().getMostSignificantBits()).putLong(identity.clusterId().getLeastSignificantBits())
            .put(identity.voterHash());id.writeTo(out);out.putInt(payload.length).put(payload);
        out.putInt(StateJournal.crc(bytes,0,bytes.length-4));return bytes;
    }
    public MetadataImage decode(SnapshotId expected,byte[] bytes)throws IOException {
        if(bytes.length<114||bytes.length>maxBytes)throw new IOException("Invalid snapshot length");
        try {
            var in=ByteBuffer.wrap(bytes);
            if(in.getInt()!=0x51534e31||in.getShort()!=1||in.getLong()!=bytes.length)throw new IOException("Invalid snapshot header");
            if(!new UUID(in.getLong(),in.getLong()).equals(identity.clusterId()))throw new IOException("Snapshot cluster mismatch");
            byte[] hash=new byte[32];in.get(hash);if(!Arrays.equals(hash,identity.voterHash()))throw new IOException("Snapshot voter mismatch");
            SnapshotId actual=SnapshotId.readFrom(in);if(!actual.equals(expected))throw new IOException("Snapshot identity mismatch");
            int count=in.getInt();if(count<12||count!=in.remaining()-4)throw new IOException("Invalid snapshot payload size");
            if(ByteBuffer.wrap(bytes).getInt(bytes.length-4)!=StateJournal.crc(bytes,0,bytes.length-4))throw new IOException("Snapshot checksum mismatch");
            byte[] payload=new byte[count];in.get(payload);var image=MetadataImageCodec.decode(payload);
            if(image.appliedOffset()!=expected.endOffset())throw new IOException("Snapshot boundary mismatch");return image;
        }catch(java.nio.BufferUnderflowException|IllegalArgumentException e){throw new IOException("Invalid snapshot",e);}
    }
    public MetadataImage load(SnapshotId id)throws IOException {
        long length=Files.size(path(id));if(length>maxBytes)throw new IOException("Snapshot exceeds limit");
        return decode(id,Files.readAllBytes(path(id)));
    }
    public List<SnapshotId> retained() { return retained; }
    public Path path(SnapshotId id) { return directory.resolve(id.contentId()+".snapshot"); }
    public Pin pin(SnapshotId id)throws IOException {
        load(id); FileChannel channel=FileChannel.open(path(id),READ);
        pins.merge(id,1,Integer::sum);return new Pin(id,channel);
    }
    public final class Pin implements AutoCloseable {
        private final SnapshotId id;private final FileChannel channel;private boolean closed;
        private Pin(SnapshotId id,FileChannel channel){this.id=id;this.channel=channel;}
        public long length()throws IOException{return channel.size();}
        public byte[] read(long position,int maxBytes)throws IOException {
            if(closed||position<0||maxBytes<=0||maxBytes>256*1024||position>channel.size())throw new IllegalArgumentException("Invalid snapshot chunk");
            var buffer=ByteBuffer.allocate((int)Math.min(maxBytes,channel.size()-position));
            while(buffer.hasRemaining()){int n=channel.read(buffer,position);if(n<=0)throw new IOException("Snapshot read stalled");position+=n;}
            return buffer.array();
        }
        @Override public void close()throws IOException {
            if(closed)return;closed=true;channel.close();pins.computeIfPresent(id,(key,count)->count==1?null:count-1);
        }
    }
}

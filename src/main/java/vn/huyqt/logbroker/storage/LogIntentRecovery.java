package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import static java.nio.file.StandardOpenOption.*;

/** Replays an externally durable truncate intent before normal recovery. Caller owns exclusive root lock. */
public final class LogIntentRecovery {
    private LogIntentRecovery() {}
    public static void truncate(Path directory,LogConfig config,long start,long committed,long target,
            DirectoryDurability directories) throws IOException {
        if(target<committed||target<start)throw new CorruptLogException("Truncate intent crosses committed prefix");
        List<Path> paths;
        try(var stream=Files.list(directory)) {
            paths=stream.filter(p->p.getFileName().toString().matches("[0-9]{20}\\.log"))
                .sorted(Comparator.comparing(p->p.getFileName().toString())).toList();
        }
        long expected=start; Path retained=null; long retainedPosition=0;
        for(var path:paths) {
            long base=Long.parseLong(path.getFileName().toString().substring(0,20));
            if(base!=expected)throw new CorruptLogException("Intent recovery segment gap");
            try(var channel=FileChannel.open(path,READ)) {
                long position=0;
                if(expected==target) { retained=path; retainedPosition=0; break; }
                while(position<channel.size()) {
                    if(channel.size()-position<30)throw new CorruptLogException("Committed intent prefix incomplete");
                    byte[] header=read(channel,position,30); BatchCodec.validateHeaderPrefix(header,config.maxBatchBytes());
                    int length=ByteBuffer.wrap(header).getInt(6);
                    if(length>channel.size()-position)throw new CorruptLogException("Intent prefix incomplete");
                    var batch=BatchCodec.decode(read(channel,position,length),config.maxBatchBytes());
                    if(batch.baseOffset()!=expected||batch.nextOffset()>target)throw new CorruptLogException("Intent target not batch boundary");
                    expected=batch.nextOffset(); position+=length;
                    if(expected==target) { retained=path; retainedPosition=position; break; }
                }
                if(retained!=null)break;
            }
        }
        if(retained==null)throw new CorruptLogException("Truncate intent prefix missing");
        for(var path:paths.reversed()) {
            if(path.getFileName().toString().compareTo(retained.getFileName().toString())<=0)break;
            Files.delete(path); Files.deleteIfExists(path.resolveSibling(path.getFileName().toString().replace(".log",".index")));
        }
        try(var channel=FileChannel.open(retained,WRITE)) { channel.truncate(retainedPosition); channel.force(true); }
        directories.sync(directory);
    }
    private static byte[] read(FileChannel channel,long position,int size)throws IOException {
        var buffer=ByteBuffer.allocate(size);
        while(buffer.hasRemaining()) { int n=channel.read(buffer,position);if(n<=0)throw new IOException("No intent recovery read progress");position+=n; }
        return buffer.array();
    }
}

package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class ScriptedLogIo extends LogIo {
    int maxWriteBytes = Integer.MAX_VALUE;
    int forceCalls;
    boolean failForce;
    long failAfterBytes = Long.MAX_VALUE;
    long writtenBytes;

    @Override int write(FileChannel channel, ByteBuffer src, long position) throws IOException {
        if (writtenBytes >= failAfterBytes) throw new IOException("Injected write failure");
        int limit = src.limit();
        int allowed = (int) Math.min(src.remaining(), Math.min(maxWriteBytes, failAfterBytes - writtenBytes));
        src.limit(src.position() + allowed);
        try {
            int n = super.write(channel, src, position);
            writtenBytes += n;
            return n;
        } finally {
            src.limit(limit);
        }
    }

    @Override void force(FileChannel channel) throws IOException {
        forceCalls++;
        if (failForce) throw new IOException("Injected force failure");
        super.force(channel);
    }
}

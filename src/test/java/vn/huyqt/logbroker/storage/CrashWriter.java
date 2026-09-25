package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CrashWriter {
    private CrashWriter() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Expected directory and mode");
        Path directory = Path.of(args[0]);
        String mode = args[1];
        var config = new LogConfig(4096, 1024, 64);
        AtomicBoolean partial = new AtomicBoolean(false);
        LogIo io = new LogIo() {
            @Override int write(FileChannel channel, ByteBuffer src, long position) throws IOException {
                if (partial.compareAndSet(true, false)) {
                    int limit = src.limit();
                    src.limit(src.position() + 1);
                    int n;
                    try { n = super.write(channel, src, position); }
                    finally { src.limit(limit); }
                    readyAndBlock();
                    return n;
                }
                return super.write(channel, src, position);
            }
        };
        PartitionLog log = PartitionLog.open(directory, config, io);
        switch (mode) {
            case "flushed" -> {
                log.append(StorageFixtures.records("a"));
                log.flush();
                readyAndBlock();
            }
            case "unflushed" -> {
                log.append(StorageFixtures.records("a"));
                log.flush();
                log.append(StorageFixtures.records("b"));
                readyAndBlock();
            }
            case "partial" -> {
                log.append(StorageFixtures.records("a"));
                log.flush();
                partial.set(true);
                log.append(StorageFixtures.records("b"));
            }
            case "lock" -> readyAndBlock();
            default -> throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }

    static void readyAndBlock() throws IOException {
        System.out.println("READY");
        System.out.flush();
        System.in.read();
    }
}

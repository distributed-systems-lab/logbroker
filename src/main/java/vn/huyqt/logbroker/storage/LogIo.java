package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The mutating file operations used by storage, kept overridable so tests can inject short
 * writes, zero progress, and force or delete failures. Production code uses this class as is.
 */
class LogIo {
    int write(FileChannel channel, ByteBuffer src, long position) throws IOException {
        return channel.write(src, position);
    }

    void force(FileChannel channel) throws IOException {
        channel.force(true);
    }

    void truncate(FileChannel channel, long size) throws IOException {
        channel.truncate(size);
    }

    void delete(Path path) throws IOException {
        Files.delete(path);
    }
}

package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

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

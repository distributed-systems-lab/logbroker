package vn.huyqt.logbroker.controller.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import static java.nio.file.StandardOpenOption.*;

/** Strict filesystem publication boundary. Unsupported providers fail rather than weaken durability. */
public class DurableFiles {
    public void forceFile(Path path) throws IOException {
        try (var channel = FileChannel.open(path, READ, WRITE)) { channel.force(true); }
    }
    public void syncDirectory(Path path) throws IOException {
        try (var channel = FileChannel.open(path, READ)) { channel.force(true); }
        catch (IOException | UnsupportedOperationException e) {
            throw new IOException("Directory durability unsupported on " + path.toAbsolutePath(), e);
        }
    }
    public void writeNew(Path path, byte[] bytes) throws IOException {
        try (var channel = FileChannel.open(path, CREATE_NEW, WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) if (channel.write(buffer) <= 0) throw new IOException("No write progress");
        }
        forceFile(path);
    }
    public void verifySupport(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) throw new IOException("Missing durability probe directory");
        syncDirectory(directory);
    }
}

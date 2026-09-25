package vn.huyqt.logbroker.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Objects;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

final class LogSegment implements AutoCloseable {
    private final Path path;
    private final long baseOffset;
    private final LogIo io;
    private final FileChannel channel;

    private LogSegment(Path path, long baseOffset, LogIo io, FileChannel channel) {
        this.path = path;
        this.baseOffset = baseOffset;
        this.io = io;
        this.channel = channel;
    }

    static LogSegment open(Path path, long baseOffset, LogIo io) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(io, "io");
        if (baseOffset < 0) throw new IllegalArgumentException("Negative base offset");
        return new LogSegment(path, baseOffset, io, FileChannel.open(path, READ, WRITE, CREATE));
    }

    static Path dataPath(Path directory, long baseOffset) {
        if (baseOffset < 0) throw new IllegalArgumentException("Negative base offset");
        return directory.resolve(String.format(Locale.ROOT, "%020d.log", baseOffset));
    }

    Path path() { return path; }
    long baseOffset() { return baseOffset; }
    long size() throws IOException { return channel.size(); }

    long append(byte[] batch) throws IOException {
        Objects.requireNonNull(batch, "batch");
        long position = channel.size();
        long start = position;
        ByteBuffer buffer = ByteBuffer.wrap(batch);
        int zeroProgress = 0;
        while (buffer.hasRemaining()) {
            int n = io.write(channel, buffer, position);
            if (n < 0 || (n == 0 && ++zeroProgress >= 16)) {
                throw new IOException("Write made no progress at " + path + ":" + position);
            }
            if (n > 0) zeroProgress = 0;
            try {
                position = Math.addExact(position, n);
            } catch (ArithmeticException e) {
                throw new IOException("File position overflow", e);
            }
        }
        return start;
    }

    byte[] readBytes(long position, int length) throws IOException {
        if (position < 0 || length < 0) throw new IllegalArgumentException("Invalid read range");
        ByteBuffer buffer = ByteBuffer.allocate(length);
        int zeroProgress = 0;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, position);
            if (n < 0) throw new EOFException("EOF at " + path + ":" + position);
            if (n == 0 && ++zeroProgress >= 16) throw new IOException("Read made no progress");
            if (n > 0) {
                zeroProgress = 0;
                position += n;
            }
        }
        return buffer.array();
    }

    void force() throws IOException { io.force(channel); }
    void truncate(long bytes) throws IOException { io.truncate(channel, bytes); }
    @Override public void close() throws IOException { channel.close(); }
}

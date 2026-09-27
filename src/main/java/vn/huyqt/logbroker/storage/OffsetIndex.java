package vn.huyqt.logbroker.storage;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Sparse offset-to-byte-position index derived entirely from the data segment.
 */
final class OffsetIndex {
    record Entry(long offset, long position) {
    }

    private final int intervalBytes;
    private final List<Entry> entries = new ArrayList<>();
    private long lastOffset = -1;
    private long lastPosition = -1;

    OffsetIndex(int intervalBytes) {
        if (intervalBytes <= 0)
            throw new IllegalArgumentException("Invalid index interval");
        this.intervalBytes = intervalBytes;
    }

    void consider(long offset, long position) {
        if (offset < 0 || position < 0 || offset <= lastOffset || position <= lastPosition) {
            throw new IllegalArgumentException("Index positions must increase");
        }
        // Always index the first batch; later entries obey the configured byte
        // interval.
        if (entries.isEmpty() || position - entries.getLast().position() >= intervalBytes) {
            entries.add(new Entry(offset, position));
        }
        lastOffset = offset;
        lastPosition = position;
    }

    Entry floor(long offset) {
        int lo = 0, hi = entries.size() - 1, found = -1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            if (entries.get(mid).offset() <= offset) {
                found = mid;
                lo = mid + 1;
            } else
                hi = mid - 1;
        }
        return found < 0 ? null : entries.get(found);
    }

    List<Entry> entries() {
        return List.copyOf(entries);
    }

    void write(Path path, LogIo io) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(io, "io");
        try (FileChannel channel = FileChannel.open(path, CREATE, WRITE, TRUNCATE_EXISTING)) {
            ByteBuffer bytes = ByteBuffer.allocate(entries.size() * 16).order(ByteOrder.BIG_ENDIAN);
            for (Entry entry : entries)
                bytes.putLong(entry.offset()).putLong(entry.position());
            bytes.flip();
            long position = 0;
            int noProgress = 0;
            while (bytes.hasRemaining()) {
                int n = io.write(channel, bytes, position);
                if (n < 0 || (n == 0 && ++noProgress >= 16))
                    throw new IOException("Index write made no progress");
                if (n > 0)
                    noProgress = 0;
                position += n;
            }
        }
    }

    static Path indexPath(Path directory, long baseOffset) {
        if (baseOffset < 0)
            throw new IllegalArgumentException("Negative base offset");
        return directory.resolve(String.format(Locale.ROOT, "%020d.index", baseOffset));
    }
}

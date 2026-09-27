package vn.huyqt.logbroker.storage;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Validates all data segments before repairing a torn active tail or rebuilding
 * indexes.
 */
final class LogRecovery {
    private static final System.Logger LOGGER = System.getLogger(LogRecovery.class.getName());

    record SegmentInfo(long baseOffset, long validBytes, long nextOffset, OffsetIndex index) {
    }

    record Result(List<SegmentInfo> segments, long nextOffset) {
        Result {
            segments = List.copyOf(segments);
        }
    }

    private record Repair(Path path, long validBytes, long originalBytes) {
    }

    private LogRecovery() {
    }

    static Result recover(Path directory, LogConfig config, LogIo io) throws IOException {
        Objects.requireNonNull(directory, "directory");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(io, "io");
        List<Long> bases = discover(directory);
        List<SegmentInfo> infos = new ArrayList<>();
        long next = 0;
        Repair repair = null;
        for (int s = 0; s < bases.size(); s++) {
            long base = bases.get(s);
            Path path = LogSegment.dataPath(directory, base);
            if (base != next)
                throw corrupt(path, 0, "Segment base offset " + base + " != expected " + next);
            boolean active = s == bases.size() - 1;
            OffsetIndex index = new OffsetIndex(config.indexIntervalBytes());
            try (FileChannel channel = FileChannel.open(path, READ)) {
                long size = channel.size();
                long position = 0;
                while (position < size) {
                    long remaining = size - position;
                    int prefixLength = (int) Math.min(BatchCodec.HEADER_BYTES, remaining);
                    byte[] prefix = read(channel, position, prefixLength);
                    try {
                        BatchCodec.validateHeaderPrefix(prefix, config.maxBatchBytes());
                    } catch (CorruptLogException e) {
                        throw corrupt(path, position, e.getMessage(), e);
                    }
                    if (prefixLength >= 18) {
                        long batchBase = ByteBuffer.wrap(prefix).getLong(10);
                        if (batchBase != next) {
                            throw corrupt(
                                    path,
                                    position,
                                    "Batch offset " + batchBase + " != expected " + next);
                        }
                    }
                    // Only the final segment can contain a torn batch from an interrupted append.
                    if (prefixLength < BatchCodec.HEADER_BYTES) {
                        if (!active)
                            throw corrupt(path, position, "Incomplete header in sealed segment");
                        repair = new Repair(path, position, size);
                        break;
                    }
                    int length = ByteBuffer.wrap(prefix).getInt(6);
                    if (length > remaining) {
                        if (!active)
                            throw corrupt(path, position, "Incomplete batch in sealed segment");
                        repair = new Repair(path, position, size);
                        break;
                    }
                    byte[] bytes = read(channel, position, length);
                    RecordBatch batch;
                    try {
                        batch = BatchCodec.decode(bytes, config.maxBatchBytes());
                    } catch (CorruptLogException e) {
                        throw corrupt(path, position, e.getMessage(), e);
                    }
                    index.consider(batch.baseOffset(), position);
                    next = batch.nextOffset();
                    try {
                        position = Math.addExact(position, length);
                    } catch (ArithmeticException e) {
                        throw corrupt(path, position, "File position overflow", e);
                    }
                }
                if (!active && position == 0)
                    throw corrupt(path, 0, "Empty sealed segment");
                infos.add(new SegmentInfo(base, position, next, index));
            }
        }

        // Do not mutate files until every data segment has been validated.
        if (repair != null) {
            try (FileChannel channel = FileChannel.open(repair.path(), WRITE)) {
                io.truncate(channel, repair.validBytes());
            }
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "Recovered torn tail: {0}, originalBytes={1}, validBytes={2}, bytesRemoved={3}",
                    repair.path(),
                    repair.originalBytes(),
                    repair.validBytes(),
                    repair.originalBytes() - repair.validBytes());
        }
        for (SegmentInfo info : infos) {
            info.index().write(OffsetIndex.indexPath(directory, info.baseOffset()), io);
        }
        // Open data writable so force covers the validated bytes before open publishes
        // them as
        // durable.
        for (SegmentInfo info : infos) {
            try (FileChannel channel = FileChannel.open(
                    LogSegment.dataPath(directory, info.baseOffset()), READ, WRITE)) {
                io.force(channel);
            }
        }
        return new Result(infos, next);
    }

    private static List<Long> discover(Path directory) throws IOException {
        List<Long> bases = new ArrayList<>();
        try (Stream<Path> files = Files.list(directory)) {
            for (Path path : files.toList()) {
                String name = path.getFileName().toString();
                if (!name.endsWith(".log"))
                    continue;
                if (!name.matches("[0-9]{20}\\.log")) {
                    throw corrupt(path, 0, "Invalid data file name");
                }
                try {
                    bases.add(Long.parseLong(name.substring(0, 20)));
                } catch (NumberFormatException e) {
                    throw corrupt(path, 0, "Data file offset overflow", e);
                }
            }
        }
        bases.sort(Comparator.naturalOrder());
        return bases;
    }

    private static byte[] read(FileChannel channel, long position, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, position);
            if (n <= 0)
                throw new IOException("Unexpected EOF while scanning log");
            position += n;
        }
        return buffer.array();
    }

    private static CorruptLogException corrupt(Path path, long position, String reason) {
        return new CorruptLogException(path + " at byte " + position + ": " + reason);
    }

    private static CorruptLogException corrupt(
            Path path, long position, String reason, Throwable cause) {
        return new CorruptLogException(path + " at byte " + position + ": " + reason, cause);
    }
}

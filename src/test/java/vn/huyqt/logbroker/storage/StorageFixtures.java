package vn.huyqt.logbroker.storage;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import java.util.Arrays;
import java.util.List;

final class StorageFixtures {
    private StorageFixtures() {}

    static LogRecord record(String value) {
        return new LogRecord(0, null, value.getBytes(StandardCharsets.UTF_8), List.of());
    }

    static List<LogRecord> records(String... values) {
        return Arrays.stream(values).map(StorageFixtures::record).toList();
    }
    static void writeBatch(Path dir, long segmentBase, byte[] bytes) throws IOException {
        Files.write(LogSegment.dataPath(dir, segmentBase), bytes, CREATE, APPEND);
    }
}
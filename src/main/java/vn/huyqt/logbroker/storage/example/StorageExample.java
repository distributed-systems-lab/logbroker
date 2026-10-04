package vn.huyqt.logbroker.storage.example;

import vn.huyqt.logbroker.storage.LogConfig;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.PartitionLog;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Command-line walkthrough of the storage API: append, read, flush, then reopen to show that
 * recovery restores the log end.
 */
public final class StorageExample {
    private StorageExample() {}

    /** Runs the walkthrough in {@code args[0]}, which must be missing or an empty directory. */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("Expected one empty storage directory");
        }
        Path directory = Path.of(args[0]);
        if (Files.exists(directory)) {
            if (!Files.isDirectory(directory)) {
                throw new IllegalArgumentException("Storage path is not a directory: " + directory);
            }
            try (var files = Files.list(directory)) {
                if (files.findAny().isPresent()) {
                    throw new IllegalArgumentException(
                            "Storage directory must be empty: " + directory);
                }
            }
        }
        List<LogRecord> records =
                List.of(
                        new LogRecord(0, null, "hello".getBytes(StandardCharsets.UTF_8), List.of()),
                        new LogRecord(
                                1, null, "broker".getBytes(StandardCharsets.UTF_8), List.of()));
        try (var log = PartitionLog.open(directory, LogConfig.defaults())) {
            System.out.println(log.append(records));
            System.out.println("batches=" + log.read(0, 1024).size());
            System.out.println("durableEndOffset=" + log.flush());
        }
        try (var log = PartitionLog.open(directory, LogConfig.defaults())) {
            System.out.println("recoveredEndOffset=" + log.logEndOffset());
        }
    }
}

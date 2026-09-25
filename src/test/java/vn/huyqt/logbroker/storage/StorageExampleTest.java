package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

class StorageExampleTest {
    @Test
    void exampleWritesReadableData(@TempDir Path root) throws Exception {
        Path dir = root.resolve("example");
        vn.huyqt.logbroker.storage.example.StorageExample.main(new String[] {dir.toString()});
        try (var log = PartitionLog.open(dir, LogConfig.defaults())) {
            assertEquals(2, log.logEndOffset());
            assertEquals(2, log.read(0, 1024).getFirst().records().size());
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        vn.huyqt.logbroker.storage.example.StorageExample.main(
                                new String[] {dir.toString()}));
        assertThrows(
                IllegalArgumentException.class,
                () -> vn.huyqt.logbroker.storage.example.StorageExample.main(new String[0]));
        Path nonEmpty = Files.createDirectory(root.resolve("nonempty"));
        Files.writeString(nonEmpty.resolve("user.txt"), "keep");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        vn.huyqt.logbroker.storage.example.StorageExample.main(
                                new String[] {nonEmpty.toString()}));
    }
}

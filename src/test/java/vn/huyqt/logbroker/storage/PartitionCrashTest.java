package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class PartitionCrashTest {
    private static final LogConfig CONFIG = new LogConfig(4096, 1024, 64);

    @Test
    @Timeout(30)
    void flushedPrefixSurvivesKillWithSpaceInPath(@TempDir Path root) throws Exception {
        Path dir = root.resolve("directory with spaces");
        killChild(dir, "flushed", false);
        try (var log = PartitionLog.open(dir, CONFIG)) {
            assertEquals(1, log.logEndOffset());
            assertEquals(StorageFixtures.records("a"), log.read(0, 1024).getFirst().records());
        }
    }

    @Test
    @Timeout(30)
    void unflushedSuffixMayOrMayNotSurviveKill(@TempDir Path dir) throws Exception {
        killChild(dir, "unflushed", false);
        try (var log = PartitionLog.open(dir, CONFIG)) {
            assertTrue(log.logEndOffset() == 1 || log.logEndOffset() == 2);
            assertEquals(StorageFixtures.records("a"), log.read(0, 1024).getFirst().records());
            if (log.logEndOffset() == 2) {
                assertEquals(StorageFixtures.records("b"), log.read(1, 1024).getFirst().records());
            }
        }
    }

    @Test
    @Timeout(30)
    void partialTailIsRemovedAfterKill(@TempDir Path dir) throws Exception {
        killChild(dir, "partial", false);
        assertEquals(52, Files.size(LogSegment.dataPath(dir, 0)));
        try (var log = PartitionLog.open(dir, CONFIG)) {
            assertEquals(1, log.logEndOffset());
            assertEquals(StorageFixtures.records("a"), log.read(0, 1024).getFirst().records());
        }
        assertEquals(51, Files.size(LogSegment.dataPath(dir, 0)));
    }

    @Test
    @Timeout(30)
    void directoryLockIsReleasedByProcessExit(@TempDir Path dir) throws Exception {
        killChild(dir, "lock", true);
        try (var log = PartitionLog.open(dir, CONFIG)) {
            assertEquals(0, log.logEndOffset());
        }
    }

    private static void awaitLockRelease(Path dir) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            try (FileChannel probe =
                    FileChannel.open(dir.resolve(".lock"), StandardOpenOption.WRITE)) {
                var acquired = probe.tryLock();
                if (acquired != null) {
                    acquired.release();
                    return;
                }
            }
            if (System.nanoTime() >= deadline) {
                throw new IOException("File lock remained held after child exit: " + dir);
            }
            Thread.sleep(10);
        }
    }

    private static void killChild(Path dir, String mode, boolean checkLock) throws Exception {
        String javaName = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        String java = Path.of(System.getProperty("java.home"), "bin", javaName).toString();
        String cp =
                Path.of(
                                PartitionLog.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI())
                        + File.pathSeparator
                        + Path.of(
                                CrashWriter.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());
        Process child =
                new ProcessBuilder(
                                java, "-cp", cp, CrashWriter.class.getName(), dir.toString(), mode)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start();
        ExecutorService readerPool = Executors.newSingleThreadExecutor();
        try {
            BufferedReader output = child.inputReader(StandardCharsets.UTF_8);
            Future<String> ready = readerPool.submit(output::readLine);
            assertEquals("READY", ready.get(10, TimeUnit.SECONDS));
            if (checkLock) {
                assertThrows(IOException.class, () -> PartitionLog.open(dir, CONFIG));
            }
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS));
            awaitLockRelease(dir);
        } finally {
            if (child.isAlive()) child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS));
            try {
                child.getInputStream().close();
                child.getOutputStream().close();
                child.getErrorStream().close();
            } finally {
                readerPool.shutdownNow();
                assertTrue(readerPool.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }
}

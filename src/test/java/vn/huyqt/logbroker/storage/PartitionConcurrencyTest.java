package vn.huyqt.logbroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class PartitionConcurrencyTest {
    @Test @Timeout(30) void readerWaitsForWholeBatch(@TempDir Path dir) throws Exception {
        CountDownLatch firstByteWritten = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(false);
        LogIo io = new LogIo() {
            @Override int write(FileChannel ch, ByteBuffer src, long pos) throws IOException {
                if (armed.compareAndSet(true, false)) {
                    int limit = src.limit();
                    src.limit(src.position() + 1);
                    int n;
                    try { n = super.write(ch, src, pos); } finally { src.limit(limit); }
                    firstByteWritten.countDown();
                    try {
                        if (!releaseWrite.await(5, TimeUnit.SECONDS)) throw new IOException("Test timed out");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                    return n;
                }
                return super.write(ch, src, pos);
            }
        };
        try (var log = PartitionLog.open(dir, new LogConfig(4096, 1024, 64), io)) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                armed.set(true);
                Future<AppendResult> writer = pool.submit(() -> log.append(StorageFixtures.records("a")));
                assertTrue(firstByteWritten.await(5, TimeUnit.SECONDS));
                CountDownLatch readerStarted = new CountDownLatch(1);
                Future<List<RecordBatch>> reader = pool.submit(() -> {
                    readerStarted.countDown();
                    return log.read(0, 1024);
                });
                assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> reader.get(100, TimeUnit.MILLISECONDS));
                releaseWrite.countDown();
                assertEquals(new AppendResult(0, 1), writer.get(5, TimeUnit.SECONDS));
                assertEquals(1, reader.get(5, TimeUnit.SECONDS).getFirst().nextOffset());
            } finally {
                releaseWrite.countDown();
                pool.shutdown();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test @Timeout(30) void concurrentWritersGetUniqueOffsetsAndReadersDoNotShareCursor(@TempDir Path dir) throws Exception {
        try (var log = PartitionLog.open(dir, new LogConfig(4096, 1024, 64))) {
            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<List<AppendResult>>> writers = new ArrayList<>();
                for (int w = 0; w < 4; w++) {
                    writers.add(pool.submit(() -> {
                        start.await();
                        List<AppendResult> results = new ArrayList<>();
                        for (int i = 0; i < 25; i++) results.add(log.append(StorageFixtures.records("x")));
                        return results;
                    }));
                }
                start.countDown();
                HashSet<Long> offsets = new HashSet<>();
                for (Future<List<AppendResult>> writer : writers) {
                    for (AppendResult result : writer.get(10, TimeUnit.SECONDS)) {
                        assertEquals(result.firstOffset() + 1, result.nextOffset());
                        assertTrue(offsets.add(result.firstOffset()));
                    }
                }
                assertEquals(100, offsets.size());
                assertEquals(100, log.logEndOffset());
                List<Future<List<RecordBatch>>> readers = new ArrayList<>();
                for (int i = 0; i < 4; i++) readers.add(pool.submit(() -> log.read(0, 10000)));
                for (Future<List<RecordBatch>> reader : readers) {
                    List<RecordBatch> batches = reader.get(10, TimeUnit.SECONDS);
                    assertEquals(100, batches.size());
                    for (int i = 0; i < batches.size(); i++) assertEquals(i, batches.get(i).baseOffset());
                }
            } finally {
                pool.shutdown();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }
}

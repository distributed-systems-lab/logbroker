package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

class LogRecoveryForceTest {
    @Test
    void forcesRecoveredDataUsingWritableChannel(@TempDir Path dir) throws Exception {
        StorageFixtures.writeBatch(
                dir, 0, BatchCodec.encode(0, StorageFixtures.records("a"), 1024));
        LogIo io =
                new LogIo() {
                    @Override
                    void force(FileChannel channel) throws IOException {
                        // A zero-byte positional write checks that recovery did not hand force a
                        // read-only channel.
                        channel.write(ByteBuffer.allocate(0), 0);
                        super.force(channel);
                    }
                };
        assertEquals(1, LogRecovery.recover(dir, new LogConfig(4096, 1024, 64), io).nextOffset());
    }
}

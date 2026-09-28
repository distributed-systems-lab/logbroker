package vn.huyqt.logbroker.storage;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionStrictDurabilityTest {
    @TempDir Path root;
    @Test void directoryForceFailurePreventsFlushPublishingDurableOffset() throws Exception {
        var fail=new AtomicBoolean(); var count=new AtomicInteger();
        DirectoryDurability sync=path->{count.incrementAndGet();if(fail.get())throw new IOException("directory force failed");};
        try(var log=PartitionLog.open(root,new LogConfig(128,128,32),new LogOpenOptions(0,0,true,sync))) {
            log.append(List.of(new LogRecord(0,null,new byte[40],List.of())));
            fail.set(true);
            assertThrows(IOException.class,log::flush);
            assertTrue(count.get()>0);
            assertThrows(IllegalStateException.class,log::logEndOffset);
        }
    }
}

package vn.huyqt.logbroker.broker;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import vn.huyqt.logbroker.storage.AppendResult;
import vn.huyqt.logbroker.storage.LogConfig;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.RecordBatch;

/** The narrow disk boundary used by broker partition logic and fault tests. */
public interface PartitionStore extends AutoCloseable {
    AppendResult append(List<LogRecord> records) throws IOException;

    List<RecordBatch> read(long offset, int maxBytes) throws IOException;

    long flush() throws IOException;

    long logStartOffset();

    long logEndOffset();

    long durableEndOffset();

    @Override
    void close() throws IOException;

    @FunctionalInterface
    interface Factory {
        PartitionStore open(Path directory, LogConfig config) throws IOException;
    }
}

package vn.huyqt.logbroker.broker;

import vn.huyqt.logbroker.storage.AppendResult;
import vn.huyqt.logbroker.storage.LogConfig;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.RecordBatch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** The narrow disk boundary used by broker partition logic and fault tests. */
public interface PartitionStore extends AutoCloseable {
    /**
     * Appends one batch. Success means the write completed, not that it is durable; see {@link
     * #durableEndOffset()}.
     */
    AppendResult append(List<LogRecord> records) throws IOException;

    /**
     * Reads whole batches from {@code offset}. The first batch may start before {@code offset} and
     * may exceed {@code maxBytes}; see {@code docs/storage-format-v1.md}.
     */
    List<RecordBatch> read(long offset, int maxBytes) throws IOException;

    /**
     * Forces appended data to disk.
     *
     * @return the new exclusive {@link #durableEndOffset()}
     */
    long flush() throws IOException;

    long logStartOffset();

    /** Exclusive end of appended data, including batches that are not yet durable. */
    long logEndOffset();

    /**
     * Exclusive end of the prefix known to be forced to disk. An append that rolls a segment may
     * advance it without an explicit {@link #flush()}.
     */
    long durableEndOffset();

    @Override
    void close() throws IOException;

    /** Opens the store for one partition directory. */
    @FunctionalInterface
    interface Factory {
        PartitionStore open(Path directory, LogConfig config) throws IOException;

        /**
         * Cluster callers provide strict creation/recovery policy; legacy injected factories keep
         * their behavior.
         */
        default PartitionStore open(
                Path directory, LogConfig config, vn.huyqt.logbroker.storage.LogOpenOptions options)
                throws IOException {
            return open(directory, config);
        }
    }
}

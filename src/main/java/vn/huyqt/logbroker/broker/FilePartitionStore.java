package vn.huyqt.logbroker.broker;

import vn.huyqt.logbroker.storage.AppendResult;
import vn.huyqt.logbroker.storage.LogConfig;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.PartitionLog;
import vn.huyqt.logbroker.storage.RecordBatch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Delegates broker data operations to the Phase 1 partition log.
 *
 * <p>Format, recovery and durability semantics are those of {@link PartitionLog}; see {@code
 * docs/storage-format-v1.md}.
 */
public final class FilePartitionStore implements PartitionStore {
    private final PartitionLog log;

    private FilePartitionStore(PartitionLog log) {
        this.log = log;
    }

    /**
     * Opens and recovers the partition log in {@code directory}, holding its directory lock until
     * {@link #close()}.
     */
    public static FilePartitionStore open(Path directory, LogConfig config) throws IOException {
        return new FilePartitionStore(PartitionLog.open(directory, config));
    }

    /** Production cluster factory honors createIfMissing and strict directory publication. */
    public static PartitionStore.Factory clusterFactory() {
        return new PartitionStore.Factory() {
            public PartitionStore open(Path directory, LogConfig config) throws IOException {
                throw new IOException("Cluster log open requires explicit recovery options");
            }

            public PartitionStore open(
                    Path directory,
                    LogConfig config,
                    vn.huyqt.logbroker.storage.LogOpenOptions options)
                    throws IOException {
                return new FilePartitionStore(PartitionLog.open(directory, config, options));
            }
        };
    }

    @Override
    public AppendResult append(List<LogRecord> records) throws IOException {
        return log.append(records);
    }

    @Override
    public List<RecordBatch> read(long offset, int maxBytes) throws IOException {
        return log.read(offset, maxBytes);
    }

    @Override
    public long flush() throws IOException {
        return log.flush();
    }

    @Override
    public long logStartOffset() {
        return log.logStartOffset();
    }

    @Override
    public long logEndOffset() {
        return log.logEndOffset();
    }

    @Override
    public long durableEndOffset() {
        return log.durableEndOffset();
    }

    @Override
    public void close() throws IOException {
        log.close();
    }
}

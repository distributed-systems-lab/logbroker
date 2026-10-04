package vn.huyqt.logbroker.controller.support;

import vn.huyqt.logbroker.controller.persistence.DurableFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/** Inject filesystem failures; directory persistence here is simulated, never platform evidence. */
public final class FaultFiles extends DurableFiles {
    private int operations, failAt = -1;
    private final List<String> trace = new ArrayList<>();
    private final Map<Path, byte[]> forced = new HashMap<>();

    public void failAfter(int relativeOperation) {
        failAt = operations + relativeOperation;
    }

    public void clearFailure() {
        failAt = -1;
    }

    public List<String> trace() {
        return List.copyOf(trace);
    }

    private void boundary(String operation) throws IOException {
        trace.add(operation);
        if (++operations == failAt)
            throw new IOException("Injected filesystem failure: " + operation);
    }

    @Override
    public void syncDirectory(Path path) throws IOException {
        boundary("directory:" + path);
    }

    @Override
    public void forceFile(Path path) throws IOException {
        boundary("force:" + path);
        super.forceFile(path);
        forced.put(path.toAbsolutePath(), java.nio.file.Files.readAllBytes(path));
    }

    @Override
    public void writeNew(Path path, byte[] bytes) throws IOException {
        boundary("write:" + path);
        super.writeNew(path, bytes);
    }

    @Override
    public void verifySupport(Path path) throws IOException {
        syncDirectory(path);
    }

    public void powerLoss() throws IOException {
        for (var entry : forced.entrySet())
            java.nio.file.Files.write(entry.getKey(), entry.getValue());
    }
}

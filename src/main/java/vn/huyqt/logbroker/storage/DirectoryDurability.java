package vn.huyqt.logbroker.storage;

import java.io.IOException;
import java.nio.file.Path;

@FunctionalInterface
public interface DirectoryDurability {
    void sync(Path directory) throws IOException;
}

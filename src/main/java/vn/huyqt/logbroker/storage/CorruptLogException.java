package vn.huyqt.logbroker.storage;

import java.io.IOException;

/** Signals invalid on-disk log data encountered during read or recovery. */
public class CorruptLogException extends IOException {
    public CorruptLogException(String message) {
        super(message);
    }

    public CorruptLogException(String message, Throwable cause) {
        super(message, cause);
    }
}

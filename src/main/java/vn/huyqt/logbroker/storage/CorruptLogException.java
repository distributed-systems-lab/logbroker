package vn.huyqt.logbroker.storage;

import java.io.IOException;

/**
 * Signals invalid on-disk log data encountered during read or recovery.
 *
 * <p>Recovery never repairs data that raised this exception; only a torn tail of the active
 * segment is cut (see {@code docs/storage-format-v1.md}). {@link RecordPayloadCodec} also throws
 * it for malformed record payloads, which the protocol layer maps to its own errors.
 */
public class CorruptLogException extends IOException {
    public CorruptLogException(String message) {
        super(message);
    }

    public CorruptLogException(String message, Throwable cause) {
        super(message, cause);
    }
}

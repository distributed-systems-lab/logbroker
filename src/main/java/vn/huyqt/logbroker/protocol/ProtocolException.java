package vn.huyqt.logbroker.protocol;

import java.io.IOException;

/**
 * A codec or validation failure carrying the {@link ErrorCode} to report for it.
 *
 * <p>The message is diagnostic only; peers act on the code.
 */
public final class ProtocolException extends IOException {
    private final ErrorCode code;

    public ProtocolException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ProtocolException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}

package vn.huyqt.logbroker.protocol;

/** Stable numeric error codes of protocol version 1. */
public enum ErrorCode {
    NONE(0), INVALID_REQUEST(1), UNSUPPORTED_OPERATION(2), UNSUPPORTED_VERSION(3),
    UNKNOWN_TOPIC(4), UNKNOWN_PARTITION(5), TOPIC_ALREADY_EXISTS(6),
    OFFSET_OUT_OF_RANGE(7), BATCH_TOO_LARGE(8), OVERLOADED(9),
    REQUEST_TIMED_OUT(10), PARTITION_UNAVAILABLE(11), STORAGE_ERROR(12),
    BROKER_SHUTTING_DOWN(13);

    private final short number;

    ErrorCode(int number) { this.number = (short) number; }

    public short number() { return number; }

    public static ErrorCode fromNumber(short number) throws ProtocolException {
        for (ErrorCode code : values()) if (code.number == number) return code;
        throw new ProtocolException(INVALID_REQUEST, "Unknown error code: " + number);
    }
}

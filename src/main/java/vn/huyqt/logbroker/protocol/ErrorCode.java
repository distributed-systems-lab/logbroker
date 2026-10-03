package vn.huyqt.logbroker.protocol;

/**
 * Stable numeric error codes of protocol version 1.
 *
 * <p>The numbers are part of the wire format (see {@code docs/protocol-v1.md}); clients act on
 * the code, never on the message text.
 */
public enum ErrorCode {
    NONE(0), INVALID_REQUEST(1), UNSUPPORTED_OPERATION(2), UNSUPPORTED_VERSION(3),
    UNKNOWN_TOPIC(4), UNKNOWN_PARTITION(5), TOPIC_ALREADY_EXISTS(6),
    OFFSET_OUT_OF_RANGE(7), BATCH_TOO_LARGE(8), OVERLOADED(9),
    REQUEST_TIMED_OUT(10), PARTITION_UNAVAILABLE(11), STORAGE_ERROR(12),
    BROKER_SHUTTING_DOWN(13), CLUSTER_MISMATCH(16), STALE_BROKER_EPOCH(23), NO_ELIGIBLE_BROKER(24),
    NOT_PARTITION_LEADER(25), FENCED_BROKER(26), STALE_PARTITION_EPOCH(27);

    private final short number;

    ErrorCode(int number) {
        this.number = (short) number;
    }

    public short number() {
        return number;
    }

    /**
     * Maps a wire number back to its code.
     *
     * @throws ProtocolException with {@link #INVALID_REQUEST} if the number is not defined
     */
    public static ErrorCode fromNumber(short number) throws ProtocolException {
        for (ErrorCode code : values())
            if (code.number == number)
                return code;
        throw new ProtocolException(INVALID_REQUEST, "Unknown error code: " + number);
    }
}

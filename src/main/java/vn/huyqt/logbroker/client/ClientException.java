package vn.huyqt.logbroker.client;

import vn.huyqt.logbroker.protocol.ErrorCode;

/**
 * A failed request distinguishes local rejection from an uncertain broker
 * outcome.
 *
 * <p>The client never retries on its own. Retrying a Produce that failed with
 * {@link Outcome#UNKNOWN} can append a duplicate, because request IDs correlate responses but do
 * not deduplicate writes (see {@code docs/protocol-v1.md}).
 */
public final class ClientException extends RuntimeException {
    /**
     * What the caller may assume about the broker's view of the failed request.
     *
     * <p>{@code NOT_SENT}: the frame was never handed to the transport, so the broker cannot
     * have applied it. {@code UNKNOWN}: writing the frame may have started, or the broker
     * returned an entry error for a Produce; the request may or may not have taken effect.
     */
    public enum Outcome {
        NOT_SENT, UNKNOWN
    }

    private final Outcome outcome;
    private final ErrorCode code;

    public ClientException(Outcome outcome, ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.outcome = outcome;
        this.code = code;
    }

    public Outcome outcome() {
        return outcome;
    }

    /** Broker error code when the failure came from a broker result, otherwise {@code null}. */
    public ErrorCode code() {
        return code;
    }

    /** Local rejection before anything was written to the connection. */
    public static ClientException notSent(String message) {
        return new ClientException(Outcome.NOT_SENT, null, message, null);
    }

    /** Failure after the frame may have reached the broker; {@code cause} is the local error. */
    public static ClientException unknown(Throwable cause) {
        return new ClientException(Outcome.UNKNOWN, null, "Broker outcome is unknown", cause);
    }
}

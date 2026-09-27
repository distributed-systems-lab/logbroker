package vn.huyqt.logbroker.client;

import vn.huyqt.logbroker.protocol.ErrorCode;

/**
 * A failed request distinguishes local rejection from an uncertain broker
 * outcome.
 */
public final class ClientException extends RuntimeException {
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

    public ErrorCode code() {
        return code;
    }

    public static ClientException notSent(String message) {
        return new ClientException(Outcome.NOT_SENT, null, message, null);
    }

    public static ClientException unknown(Throwable cause) {
        return new ClientException(Outcome.UNKNOWN, null, "Broker outcome is unknown", cause);
    }
}

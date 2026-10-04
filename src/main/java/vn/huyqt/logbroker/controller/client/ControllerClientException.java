package vn.huyqt.logbroker.controller.client;

import vn.huyqt.logbroker.controller.protocol.QuorumError;

/**
 * Final failure of a {@link ControllerClient} invocation: the last error and whether any attempt
 * could have reached a controller.
 */
public final class ControllerClientException extends RuntimeException {
    /** Whether the failed operation might nevertheless have been applied. */
    public enum Outcome {
        /** No attempt got as far as sending a request, so the operation was not applied. */
        NOT_SENT,
        /**
         * At least one attempt handed a request to the transport. For CreateTopic the topic may
         * exist; resolve by reading metadata or retrying with the same name and partition count.
         * Set even when the final error is a definite rejection, because an earlier attempt may
         * have been applied.
         */
        UNKNOWN
    }

    private final QuorumError error;
    private final Outcome outcome;

    public ControllerClientException(QuorumError error, Outcome outcome, String message) {
        super(message);
        this.error = error;
        this.outcome = outcome;
    }

    public QuorumError error() {
        return error;
    }

    public Outcome outcome() {
        return outcome;
    }
}

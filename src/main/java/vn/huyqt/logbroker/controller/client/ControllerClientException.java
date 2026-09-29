package vn.huyqt.logbroker.controller.client;
import vn.huyqt.logbroker.controller.protocol.QuorumError;
public final class ControllerClientException extends RuntimeException {
    public enum Outcome { NOT_SENT, UNKNOWN }
    private final QuorumError error;private final Outcome outcome;
    public ControllerClientException(QuorumError error,Outcome outcome,String message){super(message);this.error=error;this.outcome=outcome;}
    public QuorumError error(){return error;}public Outcome outcome(){return outcome;}
}

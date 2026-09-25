package vn.huyqt.logbroker.storage;

import java.io.IOException;

public class CorruptLogException extends IOException {
    public CorruptLogException(String message) { super(message); }
    public CorruptLogException(String message, Throwable cause) { super(message, cause); }
}

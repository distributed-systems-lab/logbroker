package vn.huyqt.logbroker.client;

import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.protocol.Protocol;

/** Shared logical request boundary; deadlines include metadata, queueing, connects and retries. */
public interface RequestClient {
    CompletableFuture<Protocol.Response> request(Protocol.Request request);
    CompletableFuture<Protocol.Response> request(Protocol.Request request, long deadlineNanos);
}

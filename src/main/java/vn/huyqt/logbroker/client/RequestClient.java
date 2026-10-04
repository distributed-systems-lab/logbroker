package vn.huyqt.logbroker.client;

import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.protocol.Protocol;

/** Shared logical request boundary; deadlines include metadata, queueing, connects and retries. */
public interface RequestClient {
    /** Uses the client's configured timeout for the entire logical operation. */
    CompletableFuture<Protocol.Response> request(Protocol.Request request);

    /**
     * Uses an absolute deadline from the client's monotonic scheduler, not wall-clock time.
     * Discovery, connection setup and safe retries must not extend that deadline.
     */
    CompletableFuture<Protocol.Response> request(Protocol.Request request, long deadlineNanos);
}

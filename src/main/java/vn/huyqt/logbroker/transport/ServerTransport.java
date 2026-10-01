package vn.huyqt.logbroker.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.broker.RequestDispatcher;

/**
 * Server lifecycle boundary independent of a network library.
 *
 * <p>The broker core sees only decoded requests through {@link RequestDispatcher}; framing,
 * connection limits and response writing belong to the implementation. During shutdown the
 * broker calls {@link #stopAccepting()} first and {@link #closeAsync()} only after accepted work
 * has drained and been flushed, so open connections can still receive those results.
 */
public interface ServerTransport {
    /**
     * Binds a listener and starts serving requests through {@code dispatcher}.
     *
     * @return the bound address, which reveals the actual port when {@code bind} uses port 0
     * @throws IOException if the listener cannot be bound
     */
    InetSocketAddress start(InetSocketAddress bind, RequestDispatcher dispatcher) throws IOException;

    /** Closes the listener; connections that are already open keep being served. */
    void stopAccepting();

    /**
     * Closes the listener and all connections and releases I/O threads. Repeated calls return
     * the same future.
     */
    CompletableFuture<Void> closeAsync();
}

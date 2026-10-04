package vn.huyqt.logbroker.transport;

import vn.huyqt.logbroker.protocol.Protocol.RequestFrame;
import vn.huyqt.logbroker.protocol.Protocol.ResponseFrame;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Client I/O boundary used by the Java client and transport conformance tests.
 *
 * <p>Implementations own framing and encoding; callers see only protocol frames and never a network
 * library type. One transport carries at most one active connection at a time. It does not
 * correlate, retry or time out requests; that is the job of {@link
 * vn.huyqt.logbroker.client.BrokerClient}.
 */
public interface ClientTransport extends AutoCloseable {
    /**
     * Opens a connection to {@code address}, replacing any previous one.
     *
     * <p>Callbacks may run on a transport I/O thread and must not block. {@code response} receives
     * each decoded reply in arrival order, which may differ from request order. {@code failure} is
     * called when the connection fails or closes, including after a reply that cannot be decoded,
     * and may be called more than once for the same connection. Both callbacks belong to this
     * connection only and may still fire after it has been replaced.
     *
     * @return future completed once the connection can accept {@link #send}, or completed
     *     exceptionally if connecting fails or the transport is closed
     */
    CompletableFuture<Void> connect(
            InetSocketAddress address,
            Consumer<ResponseFrame> response,
            Consumer<Throwable> failure);

    /**
     * Encodes and writes {@code request} on the current connection.
     *
     * @return future completed when the write has succeeded, or completed exceptionally if the
     *     frame cannot be encoded, there is no active connection, or the write fails
     */
    CompletableFuture<Void> send(RequestFrame request);

    /** Closes the connection and releases I/O resources; the transport cannot be reused. */
    @Override
    void close();
}

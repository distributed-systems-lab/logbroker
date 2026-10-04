package vn.huyqt.logbroker.controller.client;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Frame;

/**
 * One control connection. Admin callers use a fresh connection per attempt; broker callers may
 * reuse a selected controller connection for serialized RPCs. The owner correlates request IDs and
 * connection generations, so abandoned callbacks cannot reach a later attempt.
 */
public interface ControllerClientTransport extends AutoCloseable {
    /**
     * Connects to {@code address}. The callbacks may run on transport threads.
     *
     * @param receive invoked for each decoded frame
     * @param failure invoked on decode errors and disconnects; may be called more than once
     * @return completes when the connection is ready to send
     */
    CompletableFuture<Void> connect(
            InetSocketAddress address, Consumer<Frame> receive, Consumer<Throwable> failure);

    /** Writes {@code frame}; the future fails if the connection is not ready or the write fails. */
    CompletableFuture<Void> send(Frame frame);

    /**
     * Closes the connection. Callbacks already in flight may still run; {@link ControllerClient}
     * ignores those that no longer belong to the current attempt.
     */
    void close();

    /** Creates per-attempt transports and owns resources they share. */
    interface Factory extends AutoCloseable {
        ControllerClientTransport create();

        /** Releases shared resources; closing the owning {@link ControllerClient} calls this. */
        default void close() {}
    }
}

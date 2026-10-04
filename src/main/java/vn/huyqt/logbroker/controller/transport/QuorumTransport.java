package vn.huyqt.logbroker.controller.transport;

import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Network boundary between controllers, and between admin clients and a controller.
 *
 * <p>Delivers validated inbound frames to a receiver and sends requests and replies. Replies are
 * addressed by an opaque {@link ReplyRoute} that is bound to one connection incarnation, so a reply
 * can never reach a later connection that reuses the request ID. Ownership and limits are specified
 * in {@code docs/controller-protocol-v1.md}.
 */
public interface QuorumTransport {
    /**
     * Receiver owns one reference; disk consumers retain it until their completion.
     *
     * <p>A decoded inbound frame together with the resources charged for it. The release action
     * runs when the last reference is closed, returning those resources to the transport. Reference
     * counting is thread-safe.
     */
    final class Inbound implements AutoCloseable {
        private final ReplyRoute route;
        private final Frame frame;
        private final Runnable release;
        private final BooleanSupplier active;
        private final AtomicInteger references = new AtomicInteger(1);
        private Runnable reject = () -> {};

        public Inbound(ReplyRoute route, Frame frame, Runnable release) {
            this(route, frame, release, () -> true);
        }

        public Inbound(ReplyRoute route, Frame frame, Runnable release, BooleanSupplier active) {
            this.route = route;
            this.frame = frame;
            this.release = release;
            this.active = active;
        }

        public Inbound(ReplyRoute route, Frame frame) {
            this(route, frame, () -> {});
        }

        public ReplyRoute route() {
            return route;
        }

        public Frame frame() {
            return frame;
        }

        /** Returns whether the originating connection is still open. */
        public boolean isActive() {
            return active.getAsBoolean();
        }

        /** Sets the action {@link #reject} runs; the Netty transport closes the connection. */
        public Inbound rejectWith(Runnable action) {
            reject = action;
            return this;
        }

        /** Signals that the frame will not be processed. Does not release this reference. */
        public void reject() {
            reject.run();
        }

        /**
         * Adds a reference that must be closed separately.
         *
         * @throws IllegalStateException if every reference has already been released
         */
        public Inbound retain() {
            int count;
            do {
                count = references.get();
                if (count <= 0) throw new IllegalStateException("Inbound already released");
            } while (!references.compareAndSet(count, count + 1));
            return this;
        }

        /**
         * Releases one reference; the last one runs the release action.
         *
         * @throws IllegalStateException if more references are closed than were held
         */
        @Override
        public void close() {
            int left = references.decrementAndGet();
            if (left == 0) release.run();
            if (left < 0) throw new IllegalStateException("Inbound released twice");
        }
    }

    /** Failure attributed to one connection. */
    record ConnectionFailure(long connectionId, int peerId, Throwable failure) {}

    /**
     * Binds the listener and starts delivering inbound frames. The receiver must close each {@link
     * Inbound} it is given.
     *
     * @param failure receives connection-level failures
     * @return the bound address
     * @throws IOException if the listener cannot be bound
     */
    InetSocketAddress start(
            InetSocketAddress bind, Consumer<Inbound> receiver, Consumer<Throwable> failure)
            throws IOException;

    /**
     * Sends a request frame to a voter. The future completes when the frame is written, not when a
     * reply arrives; replies are delivered to the receiver.
     */
    CompletableFuture<Void> send(int peerId, Frame frame);

    /**
     * Sends a response on the connection that carried the request.
     *
     * @return a future that fails if the route's connection is closed or does not match the frame
     */
    CompletableFuture<Void> reply(ReplyRoute route, Frame frame);

    /** Closes the listener; existing connections stay open. */
    void stopAccepting();

    /** Stops accepting, closes all connections and releases transport threads. */
    CompletableFuture<Void> closeAsync();
}

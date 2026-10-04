package vn.huyqt.logbroker.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolCodec;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.transport.ClientTransport;

/**
 * Correlates one broker connection without replaying uncertain requests.
 *
 * <p>The client connects lazily on the first request and again on the first request after a
 * connection failure. Each connection has a generation number; transport callbacks and pending
 * requests are bound to the generation they were created for, so a late reply or failure from an
 * older connection cannot complete a newer request. Request IDs increase monotonically and are
 * never reused, not even across reconnects. Requests that failed are never resent.
 *
 * <p>The client owns the {@link ClientTransport} and closes it on {@link #close()}; the {@link
 * DeadlineScheduler} is owned by the caller. All state is guarded by this object's monitor, and
 * transport callbacks acquire it too. Pending futures are completed while the monitor is held, so
 * stages already attached to them run on the completing thread (a transport I/O thread, a scheduler
 * thread, or the caller) with the monitor held.
 */
public final class BrokerClient implements AutoCloseable, RequestClient {
    private final ClientConfig config;
    private final ClientTransport transport;
    private final DeadlineScheduler clock;
    private final ResourceBudget queued;
    private final ProtocolCodec codec = new ProtocolCodec(ProtocolLimits.defaults());
    private final Map<Long, Pending> pending = new HashMap<>();
    private CompletableFuture<Void> connecting;
    private long generation;
    private long nextId = 1;
    private boolean closed;

    public BrokerClient(ClientConfig config, ClientTransport transport, DeadlineScheduler clock) {
        this.config = Objects.requireNonNull(config);
        this.transport = Objects.requireNonNull(transport);
        this.clock = Objects.requireNonNull(clock);
        queued = new ResourceBudget(config.queuedBytes());
    }

    /**
     * Sends {@code body} as a version 1 request and returns a future for the broker's response.
     *
     * <p>A broker error is a successful completion carrying that error in the response body. The
     * future completes exceptionally with {@link ClientException.Outcome#NOT_SENT} if the client is
     * closed, {@link ClientConfig#maxInFlight()} or {@link ClientConfig#queuedBytes()} is
     * exhausted, the connection cannot be established, or the timeout fires before the frame is
     * handed to the transport. Once sending has started, a timeout, write failure or connection
     * loss completes it with {@link ClientException.Outcome#UNKNOWN}. The timeout is {@link
     * ClientConfig#requestTimeout()} from this call. A request that fails encoding completes with
     * the codec's exception.
     *
     * <p>Cancelling the future releases its slot and buffer but does not prove the broker did not
     * act on it.
     */
    public synchronized CompletableFuture<Protocol.Response> request(Protocol.Request body) {
        return request(body, clock.nanoTime() + config.requestTimeout().toNanos());
    }

    /** Uses the caller's original absolute deadline; a reconnect never resets it. */
    public synchronized CompletableFuture<Protocol.Response> request(
            Protocol.Request body, long deadlineNanos) {
        Objects.requireNonNull(body);
        if (clock.nanoTime() >= deadlineNanos)
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Request deadline expired"));
        if (closed) return CompletableFuture.failedFuture(ClientException.notSent("Client closed"));
        if (pending.size() >= config.maxInFlight())
            return CompletableFuture.failedFuture(
                    ClientException.notSent("In-flight request limit"));
        // IDs must never be reused during a connection lifetime (docs/protocol-v1.md), so the
        // client fails the connection instead of wrapping.
        if (nextId == Long.MAX_VALUE) {
            failConnection(generation, ClientException.notSent("Request ID exhausted"));
            return CompletableFuture.failedFuture(ClientException.notSent("Request ID exhausted"));
        }
        long id = nextId++;
        short version =
                body instanceof Protocol.CreateTopic
                                || body instanceof Protocol.Metadata
                                || body instanceof Protocol.Produce
                                || body instanceof Protocol.Fetch
                        ? (short) 1
                        : (short) 2;
        var frame = new Protocol.RequestFrame(operation(body), version, id, body);
        final int bytes;
        try {
            bytes = codec.encodeRequest(frame).length;
        } catch (Exception invalid) {
            return CompletableFuture.failedFuture(invalid);
        }
        var lease = queued.reserve(bytes).orElse(null);
        if (lease == null)
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Client request buffer full"));
        var item = new Pending(frame, lease, generation);
        pending.put(id, item);
        // The deadline starts now, so time spent connecting counts against the request timeout.
        item.timer = clock.schedule(deadlineNanos, () -> timeout(id, item));
        item.result.whenComplete(
                (ignored, error) -> {
                    if (item.result.isCancelled()) cancel(id, item);
                });
        if (connecting == null) connect();
        var connection = connecting;
        long assignedGeneration = generation;
        item.generation = assignedGeneration;
        connection.whenComplete(
                (ignored, error) -> {
                    if (error != null) failConnection(assignedGeneration, error);
                    else send(id, item);
                });
        return item.result;
    }

    // Callbacks capture the new generation so events from a replaced connection are ignored.
    private void connect() {
        long selected = ++generation;
        try {
            connecting =
                    transport.connect(
                            config.address(),
                            reply -> receive(selected, reply),
                            error -> failConnection(selected, error));
        } catch (Throwable error) {
            connecting = CompletableFuture.failedFuture(error);
        }
    }

    private synchronized void send(long id, Pending item) {
        if (pending.get(id) != item || closed || item.generation != generation) return;
        // Marked before the write starts: from here a failure may leave a broker-side effect,
        // so it must be reported as UNKNOWN rather than NOT_SENT.
        item.mayHaveSent = true;
        try {
            transport
                    .send(item.frame)
                    .whenComplete(
                            (ignored, error) -> {
                                if (error != null)
                                    failOne(id, item, ClientException.unknown(error));
                            });
        } catch (Throwable error) {
            failOne(id, item, ClientException.unknown(error));
        }
    }

    private synchronized void receive(long selected, Protocol.ResponseFrame reply) {
        if (selected != generation || closed) return;
        var item = pending.get(reply.requestId());
        if (item == null) return; // Late reply for a timed-out or cancelled request.
        // A reply must echo the request's operation and version (docs/protocol-v1.md). Fail
        // closed on a mismatch: every pending request on this connection fails.
        if (reply.operation() != item.frame.operation()
                || reply.version() != item.frame.version()) {
            failConnection(selected, new IllegalStateException("Mismatched response envelope"));
            transport.close();
            return;
        }
        release(reply.requestId(), item);
        item.result.complete(reply.body());
    }

    private synchronized void timeout(long id, Pending item) {
        failOne(
                id,
                item,
                item.mayHaveSent
                        ? ClientException.unknown(
                                new java.util.concurrent.TimeoutException("Request timed out"))
                        : ClientException.notSent("Request timed out before send"));
    }

    private synchronized void cancel(long id, Pending item) {
        release(id, item);
    }

    private synchronized void failOne(long id, Pending item, Throwable error) {
        if (release(id, item)) item.result.completeExceptionally(error);
    }

    private boolean release(long id, Pending item) {
        if (pending.get(id) != item) return false;
        pending.remove(id);
        item.timer.cancel();
        item.lease.close();
        return true;
    }

    // Reports from an older generation are ignored. Clearing connecting makes the next request
    // open a new connection with a new generation.
    private synchronized void failConnection(long selected, Throwable error) {
        if (selected != generation) return;
        connecting = null;
        for (var entry : Map.copyOf(pending).entrySet()) {
            var item = entry.getValue();
            failOne(
                    entry.getKey(),
                    item,
                    item.mayHaveSent
                            ? ClientException.unknown(error)
                            : new ClientException(
                                    ClientException.Outcome.NOT_SENT,
                                    null,
                                    "Could not connect to broker",
                                    error));
        }
    }

    /**
     * Fails every pending request (with {@link ClientException.Outcome#UNKNOWN} if it may have been
     * sent) and closes the transport. Later requests fail with {@link
     * ClientException.Outcome#NOT_SENT}. Idempotent.
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        failConnection(generation, new IllegalStateException("Client closed"));
        transport.close();
    }

    private static short operation(Protocol.Request request) {
        return switch (request) {
            case Protocol.CreateTopic ignored -> 1;
            case Protocol.Metadata ignored -> 2;
            case Protocol.Produce ignored -> 3;
            case Protocol.Fetch ignored -> 4;
            case vn.huyqt.logbroker.protocol.ClusterProtocol.CreateTopic ignored -> 1;
            case vn.huyqt.logbroker.protocol.ClusterProtocol.Metadata ignored -> 2;
            case vn.huyqt.logbroker.protocol.ClusterProtocol.Produce ignored -> 3;
            case vn.huyqt.logbroker.protocol.ClusterProtocol.Fetch ignored -> 4;
        };
    }

    /** One in-flight request, its buffer lease and timer, bound to a connection generation. */
    private static final class Pending {
        final Protocol.RequestFrame frame;
        final ResourceBudget.Lease lease;
        final CompletableFuture<Protocol.Response> result = new CompletableFuture<>();
        DeadlineScheduler.Ticket timer;
        long generation;
        boolean mayHaveSent;

        Pending(Protocol.RequestFrame frame, ResourceBudget.Lease lease, long generation) {
            this.frame = frame;
            this.lease = lease;
            this.generation = generation;
        }
    }
}

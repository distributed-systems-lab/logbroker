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

/** Correlates one broker connection without replaying uncertain requests. */
public final class BrokerClient implements AutoCloseable {
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

    public synchronized CompletableFuture<Protocol.Response> request(Protocol.Request body) {
        Objects.requireNonNull(body);
        if (closed)
            return CompletableFuture.failedFuture(ClientException.notSent("Client closed"));
        if (pending.size() >= config.maxInFlight())
            return CompletableFuture.failedFuture(ClientException.notSent("In-flight request limit"));
        if (nextId == Long.MAX_VALUE) {
            failConnection(generation, ClientException.notSent("Request ID exhausted"));
            return CompletableFuture.failedFuture(ClientException.notSent("Request ID exhausted"));
        }
        long id = nextId++;
        var frame = new Protocol.RequestFrame(operation(body), (short) 1, id, body);
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
        item.timer = clock.schedule(clock.nanoTime() + config.requestTimeout().toNanos(),
                () -> timeout(id, item));
        item.result.whenComplete((ignored, error) -> {
            if (item.result.isCancelled())
                cancel(id, item);
        });
        if (connecting == null)
            connect();
        var connection = connecting;
        long assignedGeneration = generation;
        item.generation = assignedGeneration;
        connection.whenComplete((ignored, error) -> {
            if (error != null)
                failConnection(assignedGeneration, error);
            else
                send(id, item);
        });
        return item.result;
    }

    private void connect() {
        long selected = ++generation;
        try {
            connecting = transport.connect(config.address(),
                    reply -> receive(selected, reply), error -> failConnection(selected, error));
        } catch (Throwable error) {
            connecting = CompletableFuture.failedFuture(error);
        }
    }

    private synchronized void send(long id, Pending item) {
        if (pending.get(id) != item || closed || item.generation != generation)
            return;
        item.mayHaveSent = true;
        try {
            transport.send(item.frame).whenComplete((ignored, error) -> {
                if (error != null)
                    failOne(id, item, ClientException.unknown(error));
            });
        } catch (Throwable error) {
            failOne(id, item, ClientException.unknown(error));
        }
    }

    private synchronized void receive(long selected, Protocol.ResponseFrame reply) {
        if (selected != generation || closed)
            return;
        var item = pending.get(reply.requestId());
        if (item == null)
            return; // Late reply for a timed-out or cancelled request.
        if (reply.operation() != item.frame.operation() || reply.version() != item.frame.version()) {
            failConnection(selected, new IllegalStateException("Mismatched response envelope"));
            transport.close();
            return;
        }
        release(reply.requestId(), item);
        item.result.complete(reply.body());
    }

    private synchronized void timeout(long id, Pending item) {
        failOne(id, item, item.mayHaveSent
                ? ClientException.unknown(new java.util.concurrent.TimeoutException("Request timed out"))
                : ClientException.notSent("Request timed out before send"));
    }

    private synchronized void cancel(long id, Pending item) {
        release(id, item);
    }

    private synchronized void failOne(long id, Pending item, Throwable error) {
        if (release(id, item))
            item.result.completeExceptionally(error);
    }

    private boolean release(long id, Pending item) {
        if (pending.get(id) != item)
            return false;
        pending.remove(id);
        item.timer.cancel();
        item.lease.close();
        return true;
    }

    private synchronized void failConnection(long selected, Throwable error) {
        if (selected != generation)
            return;
        connecting = null;
        for (var entry : Map.copyOf(pending).entrySet()) {
            var item = entry.getValue();
            failOne(entry.getKey(), item, item.mayHaveSent
                    ? ClientException.unknown(error)
                    : new ClientException(ClientException.Outcome.NOT_SENT, null,
                            "Could not connect to broker", error));
        }
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;
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
        };
    }

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

package vn.huyqt.logbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import vn.huyqt.logbroker.broker.metadata.BrokerMetadata;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.protocol.Protocol.Error;

/**
 * Dispatches requests and preserves independent partition outcomes.
 *
 * <p>The dispatcher performs no storage I/O itself: CreateTopic goes to the metadata worker,
 * Produce and Fetch to partition lanes, and results are returned as futures. It tracks live
 * requests per connection so that {@link #disconnect} can cancel them. Thread-safe.
 */
public final class RequestDispatcher {
    /** Injectable v1 metadata boundary for historical protocol fixtures. */
    public interface MetadataHandler {
        CompletableFuture<CreateTopicReply> create(String name, int partitions);
        MetadataReply metadata(List<String> names);
    }

    private final MetadataHandler metadata;
    private final BrokerMetadata clusterMetadata;
    private final Function<TopicPartition, PartitionRuntime> runtimes;
    private final FetchCoordinator fetch;
    private final DeadlineScheduler clock;
    private final ConcurrentHashMap<Long, Set<RequestContext>> connections = new ConcurrentHashMap<>();
    private volatile boolean stopping;

    public RequestDispatcher(MetadataHandler metadata,
            Function<TopicPartition, PartitionRuntime> runtimes,
            FetchCoordinator fetch, DeadlineScheduler clock) {
        this.metadata = Objects.requireNonNull(metadata);
        this.clusterMetadata = null;
        this.runtimes = Objects.requireNonNull(runtimes);
        this.fetch = Objects.requireNonNull(fetch);
        this.clock = Objects.requireNonNull(clock);
    }

    /** Cluster composition; the v2 branches are provided by the cluster data protocol. */
    public RequestDispatcher(BrokerMetadata metadata,Function<TopicPartition,PartitionRuntime> runtimes,
        FetchCoordinator fetch,DeadlineScheduler clock) {
        this.metadata=null; this.clusterMetadata=Objects.requireNonNull(metadata);
        this.runtimes=Objects.requireNonNull(runtimes); this.fetch=Objects.requireNonNull(fetch); this.clock=Objects.requireNonNull(clock);
    }

    /**
     * Routes one decoded request. After {@link #beginShutdown()} it answers
     * {@code BROKER_SHUTTING_DOWN} without routing. The future fails with a
     * {@link CancellationException} if the request's connection closes first; the underlying
     * operation, such as an append, is not cancelled by that.
     */
    public CompletableFuture<Response> handle(RequestContext context, Request request) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(request);
        if(clusterMetadata!=null)
            return CompletableFuture.completedFuture(new Failure(new Error(ErrorCode.UNSUPPORTED_VERSION,"Cluster broker requires data protocol v2")));
        if (stopping)
            return CompletableFuture.completedFuture(new Failure(
                    new Error(ErrorCode.BROKER_SHUTTING_DOWN, "Broker closing")));
        if (context.isCancelled())
            return CompletableFuture.failedFuture(
                    new CancellationException("Connection closed"));
        connections.computeIfAbsent(context.connectionId(), ignored -> ConcurrentHashMap.newKeySet())
                .add(context);
        CompletableFuture<Response> result = switch (request) {
            case CreateTopic create -> metadata.create(create.name(), create.partitions())
                    .thenApply(reply -> reply);
            case Metadata query -> CompletableFuture.completedFuture(metadata.metadata(query.names()));
            case Produce produce -> produce(context, produce);
            case Fetch query -> fetch.fetch(context, query).thenApply(reply -> reply);
        };
        DeadlineScheduler.Ticket onCancel = context.onCancel(
                () -> result.completeExceptionally(new CancellationException("Connection closed")));
        result.whenComplete((reply, error) -> {
            onCancel.cancel();
            var active = connections.get(context.connectionId());
            if (active != null) {
                active.remove(context);
                if (active.isEmpty())
                    connections.remove(context.connectionId(), active);
            }
        });
        return result;
    }

    // Entries are independent and not atomic across partitions. The reply is assembled when all
    // entries finish or at the deadline, whichever is first; an entry still pending then is
    // reported REQUEST_TIMED_OUT with unknown outcome, while known results are kept.
    private CompletableFuture<Response> produce(RequestContext context, Produce request) {
        if (request.entries().isEmpty())
            return CompletableFuture.completedFuture(new Failure(
                    new Error(ErrorCode.INVALID_REQUEST, "Empty Produce")));
        var result = new CompletableFuture<Response>();
        var outcomes = new ProduceResult[request.entries().size()];
        long deadline = Math.min(context.deadlineNanos(), clock.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(request.timeoutMs()));
        List<CompletableFuture<?>> pending = new ArrayList<>();
        for (int i = 0; i < request.entries().size(); i++) {
            var entry = request.entries().get(i);
            PartitionRuntime runtime = runtimes.apply(entry.partition());
            if (runtime == null) {
                outcomes[i] = new ProduceResult(entry.partition(),
                        new Error(ErrorCode.UNKNOWN_PARTITION, "Unknown partition"), -1, -1);
                continue;
            }
            final int index = i;
            CompletableFuture<ProduceResult> one = runtime.produce(entry.batch(), request.ack(), deadline);
            pending.add(one.handle((outcome, error) -> {
                synchronized (outcomes) {
                    outcomes[index] = error == null ? outcome
                            : new ProduceResult(entry.partition(),
                                    new Error(ErrorCode.STORAGE_ERROR, "Partition request failed"), -1, -1);
                }
                return null;
            }));
        }
        Runnable complete = () -> {
            synchronized (outcomes) {
                if (result.isDone())
                    return;
                List<ProduceResult> entries = new ArrayList<>(outcomes.length);
                for (int i = 0; i < outcomes.length; i++) {
                    var outcome = outcomes[i];
                    if (outcome == null)
                        outcome = new ProduceResult(
                                request.entries().get(i).partition(),
                                new Error(ErrorCode.REQUEST_TIMED_OUT, "Write outcome unknown"), -1, -1);
                    entries.add(outcome);
                }
                result.complete(new ProduceReply(Error.none(), entries));
            }
        };
        DeadlineScheduler.Ticket timeout = clock.schedule(deadline, complete);
        CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                .whenComplete((ignored, error) -> complete.run());
        result.whenComplete((ignored, error) -> timeout.cancel());
        return result;
    }

    /** Cancels every live request of {@code connectionId}; parked Fetch waiters are released. */
    public void disconnect(long connectionId) {
        Set<RequestContext> active = connections.remove(connectionId);
        if (active != null)
            for (RequestContext context : active)
                context.cancel();
    }

    /**
     * Refuses new requests and closes the {@link FetchCoordinator}, completing parked Fetch
     * requests with the data available now. Admitted Produce requests keep running.
     */
    public void beginShutdown() {
        stopping = true;
        fetch.close();
    }
}

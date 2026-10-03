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
import java.util.UUID;
import vn.huyqt.logbroker.protocol.ClusterProtocol;
import vn.huyqt.logbroker.broker.cluster.ServingGate;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.client.ControllerClientException;

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
    private final UUID clusterId;
    private final ServingGate gate;
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
        this.clusterId = null; this.gate = null;
        this.runtimes = Objects.requireNonNull(runtimes);
        this.fetch = Objects.requireNonNull(fetch);
        this.clock = Objects.requireNonNull(clock);
    }

    /** Cluster admission rechecks committed route epochs and serving permission on partition lanes. */
    public RequestDispatcher(BrokerMetadata metadata,Function<TopicPartition,PartitionRuntime> runtimes,
        FetchCoordinator fetch,DeadlineScheduler clock, UUID clusterId, ServingGate gate) {
        this.metadata=null; this.clusterMetadata=Objects.requireNonNull(metadata);
        this.clusterId=Objects.requireNonNull(clusterId); this.gate=Objects.requireNonNull(gate);
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
        boolean legacy = request instanceof CreateTopic || request instanceof Metadata
                || request instanceof Produce || request instanceof Fetch;
        if (legacy != (context.version() == 1))
            return CompletableFuture.completedFuture(new Failure(new Error(ErrorCode.UNSUPPORTED_VERSION, "Version/body mismatch")));
        if (clusterMetadata != null && context.version() != 2 || clusterMetadata == null && context.version() != 1)
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
            case ClusterProtocol.CreateTopic create -> clusterCreate(context, create);
            case ClusterProtocol.Metadata query -> CompletableFuture.completedFuture(clusterMetadata(query));
            case ClusterProtocol.Produce produce -> clusterProduce(context, produce);
            case ClusterProtocol.Fetch query -> clusterId.equals(query.clusterId())
                    ? fetch.fetch(context, query, this::admission, gate::onChange).thenApply(reply -> reply)
                    : CompletableFuture.completedFuture(clusterMismatch());
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

    private Failure clusterMismatch() {
        return new Failure(new Error(ErrorCode.CLUSTER_MISMATCH, "Foreign cluster identity"));
    }
    private CompletableFuture<Response> clusterCreate(RequestContext context, ClusterProtocol.CreateTopic request) {
        if (!clusterId.equals(request.clusterId())) return CompletableFuture.completedFuture(clusterMismatch());
        long deadline = Math.min(context.deadlineNanos(), clock.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(request.timeoutMs()));
        return clusterMetadata.create(request.name(), request.partitions(), deadline).<Response>handle((created, failure) -> {
            if (failure == null) return new ClusterProtocol.CreateTopicReply(Error.none(), created.topicId(), created.committedOffset());
            while (failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null) failure = failure.getCause();
            ErrorCode code = ErrorCode.INVALID_REQUEST;
            if (failure instanceof ControllerClientException control) {
                code = switch (control.error()) {
                    case TOPIC_ALREADY_EXISTS -> ErrorCode.TOPIC_ALREADY_EXISTS;
                    case NO_ELIGIBLE_BROKER -> ErrorCode.NO_ELIGIBLE_BROKER;
                    case CLUSTER_MISMATCH -> ErrorCode.CLUSTER_MISMATCH;
                    case OVERLOADED -> ErrorCode.OVERLOADED;
                    default -> ErrorCode.REQUEST_TIMED_OUT;
                };
            }
            return new Failure(new Error(code, "Controller topic command failed"));
        });
    }
    private Response clusterMetadata(ClusterProtocol.Metadata request) {
        if (!request.clusterId().equals(ClusterProtocol.UNPINNED) && !clusterId.equals(request.clusterId())) return clusterMismatch();
        var image = clusterMetadata.image();
        var brokers = image.brokers().entrySet().stream().map(entry -> {
            var view = entry.getValue(); var registration = view.registration();
            return new ClusterProtocol.BrokerInfo(entry.getKey(), registration.endpoint(), registration.session().brokerEpoch(), view.fenced());
        }).toList();
        var topics = new ArrayList<ClusterProtocol.TopicInfo>();
        for (var topic : image.topics()) {
            if (!request.names().isEmpty() && !request.names().contains(topic.name())) continue;
            var partitions = new ArrayList<ClusterProtocol.PartitionInfo>();
            for (int i = 0; i < topic.partitions(); i++) {
                var record = image.partitions().get(new MetadataImage.PartitionKey(topic.id(), i));
                var owner = image.brokers().get(record.leaderId());
                Error error = owner.fenced() ? new Error(ErrorCode.FENCED_BROKER, "Owner fenced") : Error.none();
                if (record.leaderId() == gate.session().brokerId() && runtimes.apply(new TopicPartition(topic.id(), i)) == null)
                    error = new Error(ErrorCode.PARTITION_UNAVAILABLE, "Local partition not ready");
                partitions.add(new ClusterProtocol.PartitionInfo(i, error, record.replicas(), record.leaderId(), record.leaderEpoch(), record.partitionEpoch()));
            }
            topics.add(new ClusterProtocol.TopicInfo(topic.name(), topic.id(), partitions));
        }
        return new ClusterProtocol.MetadataReply(Error.none(), clusterId, image.appliedOffset(), brokers, topics);
    }
    private ErrorCode admission(ClusterProtocol.Route route) {
        if (stopping) return ErrorCode.BROKER_SHUTTING_DOWN;
        var session = gate.session();
        if (route.brokerId() != session.brokerId()) return ErrorCode.NOT_PARTITION_LEADER;
        if (route.brokerEpoch() != session.brokerEpoch()) return ErrorCode.STALE_BROKER_EPOCH;
        if (!gate.canServe()) return ErrorCode.FENCED_BROKER;
        var image = clusterMetadata.image(); var view = image.brokers().get(session.brokerId());
        if (view == null || view.fenced() || !view.registration().session().equals(session)) return ErrorCode.FENCED_BROKER;
        var assignment = image.partitions().get(new MetadataImage.PartitionKey(route.partition().topicId(), route.partition().partition()));
        if (assignment == null) return ErrorCode.UNKNOWN_PARTITION;
        if (assignment.leaderId() != session.brokerId()) return ErrorCode.NOT_PARTITION_LEADER;
        if (assignment.leaderEpoch() != route.leaderEpoch()) return ErrorCode.STALE_PARTITION_EPOCH;
        if (runtimes.apply(route.partition()) == null) return ErrorCode.PARTITION_UNAVAILABLE;
        return ErrorCode.NONE;
    }
    private CompletableFuture<Response> clusterProduce(RequestContext context, ClusterProtocol.Produce request) {
        if (!clusterId.equals(request.clusterId())) return CompletableFuture.completedFuture(clusterMismatch());
        long deadline = Math.min(context.deadlineNanos(), clock.nanoTime()
                + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(request.timeoutMs()));
        var pending = new ArrayList<CompletableFuture<ClusterProtocol.ProduceResult>>();
        for (var entry : request.entries()) {
            var route = entry.route(); var error = admission(route);
            if (error != ErrorCode.NONE) pending.add(CompletableFuture.completedFuture(new ClusterProtocol.ProduceResult(
                    route.partition(), new Error(error, "Partition admission refused"), ClusterProtocol.Outcome.REJECTED, -1, -1)));
            else {
                var runtime = runtimes.apply(route.partition());
                if (runtime == null) pending.add(CompletableFuture.completedFuture(new ClusterProtocol.ProduceResult(route.partition(),
                        new Error(ErrorCode.PARTITION_UNAVAILABLE, "Partition not ready"), ClusterProtocol.Outcome.REJECTED, -1, -1)));
                else pending.add(runtime.produce(entry.batch(), request.ack(), deadline, () -> admission(route) == ErrorCode.NONE)
                        .exceptionally(failure -> new ClusterProtocol.ProduceResult(route.partition(),
                                new Error(ErrorCode.STORAGE_ERROR, "Partition request failed"), ClusterProtocol.Outcome.UNKNOWN, -1, -1)));
            }
        }
        return CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new))
                .thenApply(unused -> new ClusterProtocol.ProduceReply(Error.none(), pending.stream().map(CompletableFuture::join).toList()));
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

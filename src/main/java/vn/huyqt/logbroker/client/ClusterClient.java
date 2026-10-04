package vn.huyqt.logbroker.client;

import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.protocol.*;
import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.transport.ClientTransport;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Bounded cluster routing. Owns endpoint clients, borrows the scheduler. All asynchronous
 * completions marshal to the scheduler before touching routing state; Produce UNKNOWN never
 * retries.
 */
public final class ClusterClient implements RequestClient, AutoCloseable {
    private final ClusterClientConfig config;
    private final ClientConfig wireConfig;
    private final Function<InetSocketAddress, ClientTransport> transports;
    private final DeadlineScheduler clock;
    private final ResourceBudget queued;
    private final ProtocolCodec codec = new ProtocolCodec(ProtocolLimits.defaults());
    private final Map<InetSocketAddress, Connection> connections = new LinkedHashMap<>();
    private final Set<Operation> operations = new HashSet<>();
    private final Set<CompletableFuture<?>> completing = new HashSet<>();
    private final List<Runnable> publications = new ArrayList<>();
    private UUID clusterId;
    private ClusterProtocol.MetadataReply image;
    private CompletableFuture<ClusterProtocol.MetadataReply> refreshing;
    private boolean closed;
    private int bootstrapCursor;

    public ClusterClient(
            ClusterClientConfig config,
            ClientConfig wireConfig,
            Function<InetSocketAddress, ClientTransport> transports,
            DeadlineScheduler clock) {
        this.config = Objects.requireNonNull(config);
        this.wireConfig = Objects.requireNonNull(wireConfig);
        this.transports = Objects.requireNonNull(transports);
        this.clock = Objects.requireNonNull(clock);
        queued = new ResourceBudget(config.queuedBytes());
        clusterId = config.expectedClusterId();
    }

    private void marshal(Runnable action) {
        clock.schedule(clock.nanoTime(), () -> runStateChange(action));
    }

    private void runStateChange(Runnable action) {
        List<Runnable> pending;
        synchronized (this) {
            action.run();
            pending = List.copyOf(publications);
            publications.clear();
        }
        // Application callbacks may acquire Producer locks and call back into routing.
        // Publish only after leaving the routing monitor to avoid reversing lock order.
        for (var publication : pending) publication.run();
    }

    private <T> void complete(CompletableFuture<T> result, T value) {
        publish(result, () -> result.complete(value));
    }

    private void fail(CompletableFuture<?> result, Throwable failure) {
        publish(result, () -> result.completeExceptionally(failure));
    }

    private void publish(CompletableFuture<?> result, Runnable publication) {
        if (result.isDone() || !completing.add(result)) return;
        publications.add(
                () -> {
                    try {
                        publication.run();
                    } finally {
                        synchronized (ClusterClient.this) {
                            completing.remove(result);
                        }
                    }
                });
    }

    public CompletableFuture<Protocol.Response> request(Protocol.Request request) {
        return request(request, clock.nanoTime() + config.operationTimeout().toNanos());
    }

    public synchronized CompletableFuture<Protocol.Response> request(
            Protocol.Request request, long deadline) {
        Objects.requireNonNull(request);
        if (closed || clock.nanoTime() >= deadline)
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Cluster client closed/deadline expired"));
        if (operations.size() >= config.maxInFlight())
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Cluster operation limit"));
        long bytes;
        try {
            short version =
                    request instanceof Protocol.CreateTopic
                                    || request instanceof Protocol.Metadata
                                    || request instanceof Protocol.Produce
                                    || request instanceof Protocol.Fetch
                            ? (short) 1
                            : (short) 2;
            short operation =
                    request instanceof Protocol.CreateTopic
                                    || request instanceof ClusterProtocol.CreateTopic
                            ? (short) 1
                            : request instanceof Protocol.Metadata
                                            || request instanceof ClusterProtocol.Metadata
                                    ? (short) 2
                                    : request instanceof Protocol.Produce
                                                    || request instanceof ClusterProtocol.Produce
                                            ? (short) 3
                                            : (short) 4;
            bytes =
                    codec.encodeRequest(new Protocol.RequestFrame(operation, version, 0, request))
                            .length;
            if (version == 1) bytes = Math.addExact(bytes, 32L + 20L * 64);
        } catch (Exception error) {
            return CompletableFuture.failedFuture(error);
        }
        var lease = queued.reserve(bytes).orElse(null);
        if (lease == null)
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Cluster byte budget exhausted"));
        var operation = new Operation(request, deadline, lease);
        operations.add(operation);
        operation.timer = clock.schedule(deadline, () -> marshal(() -> expire(operation)));
        operation.result.whenComplete((unused, error) -> marshal(() -> release(operation)));
        marshal(() -> begin(operation));
        return operation.result;
    }

    public synchronized CompletableFuture<ClusterProtocol.MetadataReply> refresh(long deadline) {
        if (closed || clock.nanoTime() >= deadline)
            return CompletableFuture.failedFuture(
                    ClientException.notSent("Metadata deadline expired"));
        if (refreshing != null) return refreshing;
        var result = new CompletableFuture<ClusterProtocol.MetadataReply>();
        refreshing = result;
        result.whenComplete(
                (unused, error) ->
                        marshal(
                                () -> {
                                    if (refreshing == result) refreshing = null;
                                }));
        var timer =
                clock.schedule(
                        deadline,
                        () ->
                                marshal(
                                        () ->
                                                fail(
                                                        result,
                                                        ClientException.notSent(
                                                                "Metadata deadline expired"))));
        result.whenComplete((unused, error) -> timer.cancel());
        marshal(() -> discover(result, deadline, 0));
        return result;
    }

    private void discover(
            CompletableFuture<ClusterProtocol.MetadataReply> result, long deadline, int attempt) {
        if (result.isDone() || completing.contains(result) || closed) return;
        var endpoints = new ArrayList<>(config.bootstrap());
        if (image != null)
            for (var broker : image.brokers()) {
                var endpoint = endpoint(broker);
                if (!endpoints.contains(endpoint)) endpoints.add(endpoint);
            }
        var target = endpoints.get(Math.floorMod(bootstrapCursor + attempt, endpoints.size()));
        send(
                        target,
                        new ClusterProtocol.Metadata(
                                clusterId == null ? ClusterProtocol.UNPINNED : clusterId,
                                List.of()),
                        Math.min(deadline, clock.nanoTime() + 2_000_000_000L))
                .whenComplete(
                        (reply, failure) ->
                                marshal(
                                        () -> {
                                            if (result.isDone()
                                                    || completing.contains(result)
                                                    || closed) return;
                                            if (failure == null
                                                    && reply
                                                            instanceof
                                                            ClusterProtocol.MetadataReply
                                                                            metadata) {
                                                if (clusterId != null
                                                        && !clusterId.equals(
                                                                metadata.clusterId())) {
                                                    fail(
                                                            result,
                                                            new ClientException(
                                                                    ClientException.Outcome
                                                                            .NOT_SENT,
                                                                    ErrorCode.CLUSTER_MISMATCH,
                                                                    "Foreign bootstrap cluster",
                                                                    null));
                                                    return;
                                                }
                                                clusterId = metadata.clusterId();
                                                if (image == null
                                                        || metadata.appliedOffset()
                                                                >= image.appliedOffset()) {
                                                    var previous = image;
                                                    image = metadata;
                                                    retireChangedEndpoints(previous, metadata);
                                                }
                                                bootstrapCursor =
                                                        Math.floorMod(
                                                                bootstrapCursor + attempt,
                                                                endpoints.size());
                                                complete(result, image);
                                                return;
                                            }
                                            if (reply instanceof Protocol.Failure error
                                                    && error.error().code()
                                                            == ErrorCode.CLUSTER_MISMATCH) {
                                                fail(
                                                        result,
                                                        new ClientException(
                                                                ClientException.Outcome.NOT_SENT,
                                                                ErrorCode.CLUSTER_MISMATCH,
                                                                "Foreign bootstrap cluster",
                                                                failure));
                                                return;
                                            }
                                            retry(
                                                    deadline,
                                                    () -> discover(result, deadline, attempt + 1));
                                        }));
    }

    private void begin(Operation operation) {
        if (!active(operation)) return;
        if (operation.request instanceof Protocol.Metadata
                || operation.request instanceof ClusterProtocol.Metadata) {
            refresh(operation.deadline)
                    .whenComplete(
                            (metadata, failure) ->
                                    marshal(
                                            () -> {
                                                if (!active(operation)) return;
                                                if (failure != null)
                                                    fail(operation.result, unwrap(failure));
                                                else {
                                                    UUID requested =
                                                            requestCluster(operation.request);
                                                    if (requested != null
                                                            && !requested.equals(clusterId))
                                                        complete(
                                                                operation.result,
                                                                new Protocol.Failure(
                                                                        new Error(
                                                                                ErrorCode
                                                                                        .CLUSTER_MISMATCH,
                                                                                "Foreign metadata cluster")));
                                                    else
                                                        complete(
                                                                operation.result,
                                                                filterMetadata(
                                                                        metadata,
                                                                        operation.request));
                                                }
                                            }));
            return;
        }
        if (image == null) {
            refresh(operation.deadline)
                    .whenComplete(
                            (metadata, failure) ->
                                    marshal(
                                            () -> {
                                                if (!active(operation)) return;
                                                if (failure != null)
                                                    fail(operation.result, unwrap(failure));
                                                else begin(operation);
                                            }));
            return;
        }
        UUID requested = requestCluster(operation.request);
        if (requested != null && !requested.equals(clusterId)) {
            complete(
                    operation.result,
                    new Protocol.Failure(
                            new Error(ErrorCode.CLUSTER_MISMATCH, "Foreign request cluster")));
            return;
        }
        if (operation.request instanceof ClusterProtocol.Produce produce) {
            operation.entries = produce.entries();
            operation.ack = produce.ack();
            startProduce(operation);
        } else if (operation.request instanceof Protocol.Produce produce) {
            operation.entries =
                    produce.entries().stream()
                            .map(
                                    entry ->
                                            new ClusterProtocol.ProduceEntry(
                                                    placeholder(entry.partition()), entry.batch()))
                            .toList();
            operation.ack = produce.ack();
            startProduce(operation);
        } else if (operation.request instanceof Protocol.CreateTopic
                || operation.request instanceof ClusterProtocol.CreateTopic) create(operation);
        else startFetch(operation);
    }

    private static UUID requestCluster(Protocol.Request request) {
        return switch (request) {
            case ClusterProtocol.CreateTopic value -> value.clusterId();
            case ClusterProtocol.Metadata value ->
                    value.clusterId().equals(ClusterProtocol.UNPINNED) ? null : value.clusterId();
            case ClusterProtocol.Produce value -> value.clusterId();
            case ClusterProtocol.Fetch value -> value.clusterId();
            default -> null;
        };
    }

    private ClusterProtocol.MetadataReply filterMetadata(
            ClusterProtocol.MetadataReply metadata, Protocol.Request request) {
        UUID requested = requestCluster(request);
        if (requested != null && !requested.equals(clusterId))
            throw new CompletionException(ClientException.notSent("Foreign metadata cluster"));
        List<String> names =
                request instanceof Protocol.Metadata legacy
                        ? legacy.names()
                        : ((ClusterProtocol.Metadata) request).names();
        return new ClusterProtocol.MetadataReply(
                metadata.error(),
                metadata.clusterId(),
                metadata.appliedOffset(),
                metadata.brokers(),
                metadata.topics().stream()
                        .filter(topic -> names.isEmpty() || names.contains(topic.name()))
                        .toList());
    }

    private void create(Operation operation) {
        if (!active(operation)) return;
        String name;
        int count;
        short rf;
        if (operation.request instanceof Protocol.CreateTopic request) {
            name = request.name();
            count = request.partitions();
            rf = 1;
        } else {
            var request = (ClusterProtocol.CreateTopic) operation.request;
            name = request.name();
            count = request.partitions();
            rf = request.replicationFactor();
        }
        var endpoint =
                image.brokers().isEmpty()
                        ? config.bootstrap().getFirst()
                        : endpoint(image.brokers().getFirst());
        operation.commandSent = true;
        send(
                        endpoint,
                        new ClusterProtocol.CreateTopic(
                                clusterId, name, count, rf, remainingMillis(operation.deadline)),
                        operation.deadline)
                .whenComplete(
                        (reply, failure) ->
                                marshal(
                                        () -> {
                                            if (!active(operation)) return;
                                            if (failure == null) {
                                                complete(operation.result, reply);
                                                return;
                                            }
                                            var cause = unwrap(failure);
                                            if (cause instanceof ClientException client
                                                    && client.outcome()
                                                            == ClientException.Outcome.NOT_SENT) {
                                                operation.commandSent = false;
                                                retry(operation.deadline, () -> create(operation));
                                            } else fail(operation.result, cause);
                                        }));
    }

    private void startProduce(Operation operation) {
        if (operation.outcomes == null) {
            operation.outcomes = new ClusterProtocol.ProduceResult[operation.entries.size()];
            operation.possiblySent = new boolean[operation.entries.size()];
        }
        produceAttempt(operation);
    }

    private void produceAttempt(Operation operation) {
        if (!active(operation)) return;
        Map<InetSocketAddress, List<Integer>> groups = new LinkedHashMap<>();
        boolean missing = false;
        for (int i = 0; i < operation.entries.size(); i++) {
            if (operation.outcomes[i] != null) continue;
            var route = route(operation.entries.get(i).route().partition());
            if (route == null) {
                missing = true;
                continue;
            }
            var broker = broker(route.brokerId());
            groups.computeIfAbsent(endpoint(broker), unused -> new ArrayList<>()).add(i);
        }
        if (missing) {
            refreshThen(operation, () -> produceAttempt(operation));
            return;
        }
        if (groups.isEmpty()) {
            finishProduce(operation);
            return;
        }
        operation.attempts = groups.size();
        operation.retryProduce = false;
        for (var group : groups.entrySet()) {
            var entries =
                    group.getValue().stream()
                            .map(
                                    index -> {
                                        var original = operation.entries.get(index);
                                        return new ClusterProtocol.ProduceEntry(
                                                route(original.route().partition()),
                                                original.batch());
                                    })
                            .toList();
            for (int index : group.getValue()) operation.possiblySent[index] = true;
            send(
                            group.getKey(),
                            new ClusterProtocol.Produce(
                                    clusterId,
                                    operation.ack,
                                    remainingMillis(operation.deadline),
                                    entries),
                            operation.deadline)
                    .whenComplete(
                            (reply, failure) ->
                                    marshal(
                                            () -> {
                                                if (!active(operation)) return;
                                                Map<
                                                                Protocol.TopicPartition,
                                                                ClusterProtocol.ProduceResult>
                                                        returned = new HashMap<>();
                                                boolean malformed = false;
                                                if (reply
                                                        instanceof
                                                        ClusterProtocol.ProduceReply produced) {
                                                    for (var result : produced.results())
                                                        if (returned.put(result.partition(), result)
                                                                != null) malformed = true;
                                                    if (produced.results().size() != entries.size()
                                                            || produced.error().code()
                                                                    != ErrorCode.NONE)
                                                        malformed = true;
                                                } else malformed = true;
                                                Throwable cause =
                                                        failure == null ? null : unwrap(failure);
                                                boolean notSent =
                                                        cause instanceof ClientException error
                                                                && error.outcome()
                                                                        == ClientException.Outcome
                                                                                .NOT_SENT;
                                                for (int index : group.getValue()) {
                                                    var partition =
                                                            operation
                                                                    .entries
                                                                    .get(index)
                                                                    .route()
                                                                    .partition();
                                                    var outcome =
                                                            malformed
                                                                    ? null
                                                                    : returned.get(partition);
                                                    if (failure != null || outcome == null)
                                                        outcome =
                                                                failureResult(
                                                                        partition,
                                                                        notSent
                                                                                ? ClusterProtocol
                                                                                        .Outcome
                                                                                        .REJECTED
                                                                                : ClusterProtocol
                                                                                        .Outcome
                                                                                        .UNKNOWN,
                                                                        failure == null
                                                                                ? ErrorCode
                                                                                        .INVALID_REQUEST
                                                                                : ErrorCode
                                                                                        .REQUEST_TIMED_OUT);
                                                    if (outcome.retrySafe()
                                                            && (notSent
                                                                    || retryable(
                                                                            outcome.error()
                                                                                    .code()))) {
                                                        operation.possiblySent[index] = false;
                                                        operation.retryProduce = true;
                                                    } else operation.outcomes[index] = outcome;
                                                }
                                                if (--operation.attempts == 0) {
                                                    if (operation.retryProduce)
                                                        refreshThen(
                                                                operation,
                                                                () -> produceAttempt(operation));
                                                    else finishProduce(operation);
                                                }
                                            }));
        }
    }

    private void refreshThen(Operation operation, Runnable action) {
        retry(
                operation.deadline,
                () -> {
                    if (!active(operation)) return;
                    refresh(operation.deadline)
                            .whenComplete(
                                    (metadata, failure) ->
                                            marshal(
                                                    () -> {
                                                        if (!active(operation)) return;
                                                        if (failure != null)
                                                            refreshThen(operation, action);
                                                        else action.run();
                                                    }));
                });
    }

    private void finishProduce(Operation operation) {
        complete(
                operation.result,
                new ClusterProtocol.ProduceReply(Error.none(), List.of(operation.outcomes)));
    }

    private void startFetch(Operation operation) {
        if (operation.request instanceof ClusterProtocol.Fetch request) {
            operation.fetchEntries = request.entries();
            operation.maxBytes = request.maxBytes();
            operation.minBytes = request.minBytes();
            operation.allowOversized = request.allowOversizedFirstBatch();
            operation.fetchDeadline =
                    Math.min(
                            operation.deadline,
                            clock.nanoTime() + request.maxWaitMs() * 1_000_000L);
        } else {
            var request = (Protocol.Fetch) operation.request;
            operation.fetchEntries =
                    request.entries().stream()
                            .map(
                                    entry ->
                                            new ClusterProtocol.FetchEntry(
                                                    placeholder(entry.partition()),
                                                    entry.offset(),
                                                    entry.maxBytes()))
                            .toList();
            operation.maxBytes = request.maxBytes();
            operation.minBytes = request.minBytes();
            operation.allowOversized = true;
            operation.fetchDeadline =
                    Math.min(
                            operation.deadline,
                            clock.nanoTime() + request.maxWaitMs() * 1_000_000L);
        }
        fetchRound(operation);
    }

    private void fetchRound(Operation operation) {
        if (!active(operation)) return;
        operation.fetchResults = new ArrayList<>();
        operation.remainingBytes = operation.maxBytes;
        operation.anyFetchData = false;
        fetchEntry(operation, 0);
    }

    private void fetchEntry(Operation operation, int index) {
        if (!active(operation)) return;
        if (index == operation.fetchEntries.size()) {
            long bytes =
                    operation.fetchResults.stream()
                            .flatMap(result -> result.batches().stream())
                            .mapToLong(batch -> WireBatchCodec.fetchSize(batch.batch()))
                            .sum();
            boolean error =
                    operation.fetchResults.stream()
                            .anyMatch(result -> result.error().code() != ErrorCode.NONE);
            if (!error
                    && bytes < operation.minBytes
                    && clock.nanoTime() < operation.fetchDeadline) {
                clock.schedule(
                        Math.min(
                                operation.fetchDeadline,
                                clock.nanoTime() + config.retryBackoff().toNanos()),
                        () -> marshal(() -> fetchRound(operation)));
                return;
            }
            var reply = new ClusterProtocol.FetchReply(Error.none(), operation.fetchResults);
            try {
                codec.encodeResponse(new Protocol.ResponseFrame((short) 4, (short) 2, 0, reply));
                complete(operation.result, reply);
            } catch (ProtocolException invalid) {
                fail(operation.result, invalid);
            }
            return;
        }
        var entry = operation.fetchEntries.get(index);
        var partition = entry.route().partition();
        if (operation.remainingBytes == 0) {
            operation.fetchResults.add(
                    fetchFailure(partition, ErrorCode.OVERLOADED, "Global Fetch budget consumed"));
            fetchEntry(operation, index + 1);
            return;
        }
        var route = route(partition);
        if (route == null) {
            refreshThen(operation, () -> fetchEntry(operation, index));
            return;
        }
        boolean delegated = operation.allowOversized && !operation.anyFetchData;
        var request =
                new ClusterProtocol.Fetch(
                        clusterId,
                        operation.remainingBytes,
                        0,
                        0,
                        List.of(
                                new ClusterProtocol.FetchEntry(
                                        route, entry.offset(), entry.maxBytes())),
                        delegated);
        send(endpoint(broker(route.brokerId())), request, operation.deadline)
                .whenComplete(
                        (reply, failure) ->
                                marshal(
                                        () -> {
                                            if (!active(operation)) return;
                                            ClusterProtocol.FetchResult fetched = null;
                                            if (failure == null
                                                    && reply
                                                            instanceof
                                                            ClusterProtocol.FetchReply response
                                                    && response.error().code() == ErrorCode.NONE
                                                    && response.results().size() == 1
                                                    && response.results()
                                                            .getFirst()
                                                            .partition()
                                                            .equals(partition))
                                                fetched = response.results().getFirst();
                                            if (fetched == null
                                                    || retryable(fetched.error().code())) {
                                                if (clock.nanoTime() < operation.deadline) {
                                                    refreshThen(
                                                            operation,
                                                            () -> fetchEntry(operation, index));
                                                    return;
                                                }
                                                fetched =
                                                        fetchFailure(
                                                                partition,
                                                                ErrorCode.REQUEST_TIMED_OUT,
                                                                "Fetch expired");
                                            }
                                            long bytes =
                                                    fetched.batches().stream()
                                                            .mapToLong(
                                                                    batch ->
                                                                            WireBatchCodec
                                                                                    .fetchSize(
                                                                                            batch
                                                                                                    .batch()))
                                                            .sum();
                                            if (bytes > operation.remainingBytes
                                                    || bytes > entry.maxBytes()) {
                                                if (!delegated || fetched.batches().size() != 1) {
                                                    fail(
                                                            operation.result,
                                                            new IllegalStateException(
                                                                    "Broker violated global Fetch budget"));
                                                    return;
                                                }
                                            }
                                            operation.remainingBytes =
                                                    (int)
                                                            Math.max(
                                                                    0,
                                                                    operation.remainingBytes
                                                                            - bytes);
                                            operation.anyFetchData |= !fetched.batches().isEmpty();
                                            operation.fetchResults.add(fetched);
                                            fetchEntry(operation, index + 1);
                                        }));
    }

    private static ClusterProtocol.FetchResult fetchFailure(
            Protocol.TopicPartition partition, ErrorCode code, String message) {
        return new ClusterProtocol.FetchResult(
                partition, new Error(code, message), -1, -1, -1, List.of());
    }

    private ClusterProtocol.Route route(Protocol.TopicPartition partition) {
        if (image == null) return null;
        for (var topic : image.topics())
            if (topic.id().equals(partition.topicId()))
                for (var part : topic.partitions())
                    if (part.partition() == partition.partition()) {
                        var broker = broker(part.leaderId());
                        if (broker == null) return null;
                        return new ClusterProtocol.Route(
                                partition, broker.id(), broker.brokerEpoch(), part.leaderEpoch());
                    }
        return null;
    }

    private ClusterProtocol.BrokerInfo broker(int id) {
        return image.brokers().stream()
                .filter(broker -> broker.id() == id)
                .findFirst()
                .orElse(null);
    }

    private static ClusterProtocol.Route placeholder(Protocol.TopicPartition partition) {
        return new ClusterProtocol.Route(partition, 0, 0, 0);
    }

    private static InetSocketAddress endpoint(ClusterProtocol.BrokerInfo broker) {
        return new InetSocketAddress(broker.endpoint().host(), broker.endpoint().port());
    }

    private CompletableFuture<Protocol.Response> send(
            InetSocketAddress target, Protocol.Request request, long deadline) {
        if (closed || clock.nanoTime() >= deadline)
            return CompletableFuture.failedFuture(ClientException.notSent("Operation expired"));
        Connection connection = connections.get(target);
        if (connection == null) {
            if (connections.size() >= config.maxConnections()) {
                var idle =
                        connections.entrySet().stream()
                                .filter(entry -> entry.getValue().busy == 0)
                                .findFirst()
                                .orElse(null);
                if (idle == null)
                    return CompletableFuture.failedFuture(
                            ClientException.notSent("Connection limit"));
                connections.remove(idle.getKey());
                idle.getValue().client.close();
            }
            var local =
                    new ClientConfig(
                            target,
                            wireConfig.queuedBytes(),
                            wireConfig.maxInFlight(),
                            wireConfig.targetBatchBytes(),
                            wireConfig.linger(),
                            wireConfig.requestTimeout());
            try {
                connection =
                        new Connection(new BrokerClient(local, transports.apply(target), clock));
            } catch (RuntimeException error) {
                return CompletableFuture.failedFuture(
                        ClientException.notSent("Transport creation failed"));
            }
            connections.put(target, connection);
        }
        var selected = connection;
        selected.busy++;
        var result = selected.client.request(request, deadline);
        result.whenComplete((unused, error) -> marshal(() -> selected.busy--));
        return result;
    }

    private void retireChangedEndpoints(
            ClusterProtocol.MetadataReply previous, ClusterProtocol.MetadataReply next) {
        if (previous == null) return;
        for (var old : previous.brokers()) {
            var current =
                    next.brokers().stream()
                            .filter(broker -> broker.id() == old.id())
                            .findFirst()
                            .orElse(null);
            if (current == null || !current.endpoint().equals(old.endpoint())) {
                var removed = connections.remove(endpoint(old));
                if (removed != null) removed.client.close();
            }
        }
    }

    private void retry(long deadline, Runnable action) {
        if (closed || clock.nanoTime() >= deadline) return;
        clock.schedule(
                Math.min(deadline, clock.nanoTime() + config.retryBackoff().toNanos()),
                () -> marshal(action));
    }

    private int remainingMillis(long deadline) {
        return (int) Math.max(1, Math.min(30000, (deadline - clock.nanoTime()) / 1_000_000));
    }

    private boolean active(Operation operation) {
        return !closed && !operation.result.isDone() && !completing.contains(operation.result);
    }

    private void expire(Operation operation) {
        if (!active(operation)) return;
        if (operation.outcomes != null) {
            for (int i = 0; i < operation.outcomes.length; i++)
                if (operation.outcomes[i] == null)
                    operation.outcomes[i] =
                            failureResult(
                                    operation.entries.get(i).route().partition(),
                                    operation.possiblySent[i]
                                            ? ClusterProtocol.Outcome.UNKNOWN
                                            : ClusterProtocol.Outcome.REJECTED,
                                    ErrorCode.REQUEST_TIMED_OUT);
            finishProduce(operation);
        } else
            fail(
                    operation.result,
                    operation.commandSent
                            ? ClientException.unknown(
                                    new TimeoutException("Topic command deadline expired"))
                            : ClientException.notSent("Operation deadline expired"));
    }

    private void release(Operation operation) {
        if (!operations.remove(operation)) return;
        operation.timer.cancel();
        operation.lease.close();
    }

    private static ClusterProtocol.ProduceResult failureResult(
            Protocol.TopicPartition partition, ClusterProtocol.Outcome outcome, ErrorCode code) {
        return new ClusterProtocol.ProduceResult(
                partition, new Error(code, "Cluster request failed"), outcome, -1, -1);
    }

    private static boolean retryable(ErrorCode code) {
        return switch (code) {
            case NOT_PARTITION_LEADER,
                            STALE_PARTITION_EPOCH,
                            STALE_BROKER_EPOCH,
                            FENCED_BROKER,
                            PARTITION_UNAVAILABLE,
                            OVERLOADED ->
                    true;
            default -> false;
        };
    }

    private static Throwable unwrap(Throwable error) {
        while (error instanceof CompletionException && error.getCause() != null)
            error = error.getCause();
        return error;
    }

    public void close() {
        runStateChange(this::closeState);
    }

    private void closeState() {
        if (closed) return;
        for (var operation : List.copyOf(operations)) expire(operation);
        closed = true;
        if (refreshing != null) fail(refreshing, ClientException.notSent("Cluster client closed"));
        for (var connection : connections.values()) connection.client.close();
        connections.clear();
        for (var operation : List.copyOf(operations)) release(operation);
    }

    private static final class Connection {
        final BrokerClient client;
        int busy;

        Connection(BrokerClient client) {
            this.client = client;
        }
    }

    private static final class Operation {
        final Protocol.Request request;
        final long deadline;
        final ResourceBudget.Lease lease;
        final CompletableFuture<Protocol.Response> result = new CompletableFuture<>();
        DeadlineScheduler.Ticket timer;
        List<ClusterProtocol.ProduceEntry> entries;
        Protocol.AckMode ack;
        ClusterProtocol.ProduceResult[] outcomes;
        boolean[] possiblySent;
        int attempts;
        boolean retryProduce;
        boolean commandSent;
        List<ClusterProtocol.FetchEntry> fetchEntries;
        List<ClusterProtocol.FetchResult> fetchResults;
        int maxBytes, minBytes, remainingBytes;
        long fetchDeadline;
        boolean allowOversized, anyFetchData;

        Operation(Protocol.Request request, long deadline, ResourceBudget.Lease lease) {
            this.request = request;
            this.deadline = deadline;
            this.lease = lease;
        }
    }
}

package vn.huyqt.logbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.protocol.Protocol.Fetch;
import vn.huyqt.logbroker.protocol.Protocol.FetchEntry;
import vn.huyqt.logbroker.protocol.Protocol.FetchReply;
import vn.huyqt.logbroker.protocol.Protocol.FetchResult;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.protocol.WireBatchCodec;

/** Assembles independent partition results under one wire-byte budget. */
public final class FetchPlanner {
    private final Function<TopicPartition, PartitionRuntime> lookup;
    private final BrokerConfig config;

    public FetchPlanner(Map<TopicPartition, PartitionRuntime> runtimes, BrokerConfig config) {
        this(runtimes::get, config);
    }

    public FetchPlanner(Function<TopicPartition, PartitionRuntime> lookup, BrokerConfig config) {
        this.lookup = Objects.requireNonNull(lookup);
        this.config = Objects.requireNonNull(config);
    }

    public CompletableFuture<FetchReply> read(Fetch request) {
        if (request.entries().isEmpty() || request.entries().size() > config.protocolLimits().maxPartitionEntries()
                || request.maxBytes() <= 0 || request.maxBytes() > config.maxFetchBytes()
                || request.minBytes() < 0 || request.minBytes() > request.maxBytes()) {
            return CompletableFuture.completedFuture(new FetchReply(
                    new Error(ErrorCode.INVALID_REQUEST, "Invalid Fetch budget"), List.of()));
        }
        var state = new State(request.maxBytes());
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (FetchEntry entry : request.entries()) {
            chain = chain.thenCompose(ignored -> {
                PartitionRuntime runtime = lookup.apply(entry.partition());
                if (runtime == null) {
                    state.results.add(new FetchResult(entry.partition(), new Error(
                            ErrorCode.UNKNOWN_PARTITION, "Unknown partition"), -1, -1, List.of()));
                    return CompletableFuture.completedFuture(null);
                }
                return runtime.read(entry, state.remaining, !state.anyData)
                        .thenAccept(result -> {
                            state.results.add(result);
                            for (var batch : result.batches()) {
                                state.remaining = Math.max(0, state.remaining
                                        - WireBatchCodec.fetchSize(batch.batch()));
                                state.anyData = true;
                            }
                        });
            });
        }
        return chain.thenApply(ignored -> new FetchReply(Error.none(), state.results));
    }

    private static final class State {
        int remaining;
        boolean anyData;
        final List<FetchResult> results = new ArrayList<>();
        State(int remaining) { this.remaining = remaining; }
    }
}

package vn.huyqt.logbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.protocol.Protocol.Fetch;
import vn.huyqt.logbroker.protocol.Protocol.FetchReply;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;
import vn.huyqt.logbroker.protocol.WireBatchCodec;

/**
 * Event-driven long polling without occupying a partition worker while waiting.
 */
public final class FetchCoordinator implements AutoCloseable {
    private final FetchPlanner planner;
    private final Function<TopicPartition, PartitionRuntime> lookup;
    private final DeadlineScheduler clock;
    private final ResourceBudget waiterBudget;
    private final Set<Waiter> waiters = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    public FetchCoordinator(FetchPlanner planner,
            Function<TopicPartition, PartitionRuntime> lookup,
            DeadlineScheduler clock, ResourceBudget waiterBudget) {
        this.planner = Objects.requireNonNull(planner);
        this.lookup = Objects.requireNonNull(lookup);
        this.clock = Objects.requireNonNull(clock);
        this.waiterBudget = Objects.requireNonNull(waiterBudget);
    }

    public CompletableFuture<FetchReply> fetch(RequestContext context, Fetch request) {
        if (closed)
            return CompletableFuture.completedFuture(new FetchReply(
                    new Error(ErrorCode.BROKER_SHUTTING_DOWN, "Broker closing"), List.of()));
        if (request.maxWaitMs() == 0 || request.minBytes() == 0)
            return planner.read(request);
        ResourceBudget.Lease lease = waiterBudget.reserve(1).orElse(null);
        if (lease == null)
            return CompletableFuture.completedFuture(new FetchReply(
                    new Error(ErrorCode.OVERLOADED, "Too many Fetch waiters"), List.of()));
        Waiter waiter = new Waiter(context, request, lease);
        waiters.add(waiter);
        waiter.start();
        return waiter.result;
    }

    @Override
    public void close() {
        closed = true;
        for (Waiter waiter : List.copyOf(waiters))
            waiter.expire();
    }

    private final class Waiter {
        final RequestContext context;
        final Fetch request;
        final ResourceBudget.Lease lease;
        final CompletableFuture<FetchReply> result = new CompletableFuture<>();
        final AtomicBoolean finished = new AtomicBoolean();
        final AtomicBoolean evaluating = new AtomicBoolean();
        final AtomicBoolean rerun = new AtomicBoolean();
        final List<DeadlineScheduler.Ticket> subscriptions = new ArrayList<>();
        DeadlineScheduler.Ticket timer;
        DeadlineScheduler.Ticket cancelHook;
        volatile boolean expired;

        Waiter(RequestContext context, Fetch request, ResourceBudget.Lease lease) {
            this.context = context;
            this.request = request;
            this.lease = lease;
        }

        synchronized void start() {
            cancelHook = context.onCancel(this::cancel);
            if (finished.get())
                return;
            // Subscribe first. Any append before or during initial read schedules another
            // evaluation.
            for (var entry : request.entries()) {
                PartitionRuntime runtime = lookup.apply(entry.partition());
                if (runtime != null)
                    subscriptions.add(runtime.onChange(this::evaluate));
            }
            long due = Math.addExact(clock.nanoTime(),
                    java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(request.maxWaitMs()));
            timer = clock.schedule(due, this::expire);
            evaluate();
        }

        void expire() {
            expired = true;
            evaluate();
        }

        void evaluate() {
            if (finished.get())
                return;
            if (!evaluating.compareAndSet(false, true)) {
                rerun.set(true);
                return;
            }
            planner.read(request).whenComplete((reply, failure) -> {
                if (failure != null) {
                    finish(null, failure);
                } else if (expired || enough(reply) || hasError(reply)) {
                    finish(reply, null);
                }
                evaluating.set(false);
                if (!finished.get() && rerun.getAndSet(false))
                    evaluate();
            });
        }

        boolean enough(FetchReply reply) {
            long bytes = 0;
            for (var partition : reply.results())
                for (var batch : partition.batches())
                    bytes += WireBatchCodec.fetchSize(batch.batch());
            return bytes >= request.minBytes();
        }

        boolean hasError(FetchReply reply) {
            if (reply.error().code() != ErrorCode.NONE)
                return true;
            return reply.results().stream().anyMatch(
                    partition -> partition.error().code() != ErrorCode.NONE);
        }

        void cancel() {
            finish(null, new CancellationException("Client disconnected"));
        }

        synchronized void finish(FetchReply reply, Throwable failure) {
            if (!finished.compareAndSet(false, true))
                return;
            if (timer != null)
                timer.cancel();
            if (cancelHook != null)
                cancelHook.cancel();
            subscriptions.forEach(DeadlineScheduler.Ticket::cancel);
            lease.close();
            waiters.remove(this);
            if (failure == null)
                result.complete(reply);
            else
                result.completeExceptionally(failure);
        }
    }
}

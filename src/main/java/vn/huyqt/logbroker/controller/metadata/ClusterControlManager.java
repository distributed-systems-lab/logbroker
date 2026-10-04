package vn.huyqt.logbroker.controller.metadata;

import java.util.*;
import java.util.function.IntPredicate;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/** Loop-owned admission/reservations; consensus alone appends, commits and supplies images. */
public final class ClusterControlManager {
    public record Completion(long invocationId, Reply reply) {}

    private static final class Pending {
        final Request request;
        final UUID topicId;
        final List<Integer> assignment;
        ClusterRecords.BrokerState lifecycle;
        Recovery recovery;
        long end = Long.MAX_VALUE;

        Pending(Request request, UUID topicId, List<Integer> assignment) {
            this.request = request;
            this.topicId = topicId;
            this.assignment = assignment;
        }
    }

    private static final UUID NO_RECOVERY = new UUID(0, 0);

    private static final class Recovery {
        final UUID id;
        final ClusterRecords.Session session;
        final long target;
        long end = Long.MAX_VALUE;

        Recovery(UUID id, ClusterRecords.Session session, long target) {
            this.id = id;
            this.session = session;
            this.target = target;
        }
    }

    private final MetadataLimits limits;
    private final IntPredicate eligible;
    private final int batchBytes;
    private MetadataImage image = MetadataImage.empty((short) 2);
    private final ArrayDeque<Pending> queue = new ArrayDeque<>();
    private final Map<Integer, Pending> registrations = new HashMap<>();
    private final Map<String, Pending> topics = new HashMap<>();
    private final Map<Long, Pending> waiters = new LinkedHashMap<>();
    private final Map<Integer, Pending> lifecycles = new HashMap<>();
    private final Map<Integer, Recovery> recoveries = new HashMap<>();
    private HeartbeatTracker heartbeatTracker;

    public ClusterControlManager(MetadataLimits limits, IntPredicate eligible) {
        this(limits, eligible, 1024 * 1024);
    }

    public ClusterControlManager(MetadataLimits limits, IntPredicate eligible, int batchBytes) {
        this.limits = Objects.requireNonNull(limits);
        this.eligible = Objects.requireNonNull(eligible);
        this.batchBytes = batchBytes;
    }

    public void onImage(MetadataImage image) {
        if (image.appliedOffset() < this.image.appliedOffset())
            throw new IllegalArgumentException("Image regression");
        this.image = image;
        registrations.entrySet().removeIf(e -> e.getValue().end <= image.appliedOffset());
        topics.entrySet().removeIf(e -> e.getValue().end <= image.appliedOffset());
        lifecycles.entrySet().removeIf(e -> e.getValue().end <= image.appliedOffset());
        recoveries
                .entrySet()
                .removeIf(
                        e -> {
                            var view = image.brokers().get(e.getKey());
                            var recovery = e.getValue();
                            return view == null
                                    || !view.registration().session().equals(recovery.session)
                                    || view.fenced() && view.stateOffset() > recovery.target;
                        });
        if (heartbeatTracker != null)
            image.brokers()
                    .values()
                    .forEach(v -> heartbeatTracker.observe(v.registration().session()));
    }

    /** Starts an observation window only after this leader's marker has applied. */
    public void leaderStarted(HeartbeatTracker tracker, long epoch, long now) {
        heartbeatTracker = tracker;
        tracker.leaderStarted(epoch, now);
        recoveries.clear();
        image.brokers().values().forEach(v -> tracker.observe(v.registration().session()));
    }

    /** Expiry reserves one durable fence; subsequent contact cannot withdraw that command. */
    public void onTick(long now) {
        if (heartbeatTracker == null) return;
        for (int id : heartbeatTracker.expired(now)) {
            var view = image.brokers().get(id);
            if (view != null
                    && !view.fenced()
                    && !lifecycles.containsKey(id)
                    && !registrations.containsKey(id)) {
                var p = new Pending(null, null, List.of());
                p.lifecycle =
                        new ClusterRecords.BrokerState(
                                id, view.registration().session().brokerEpoch(), true);
                lifecycles.put(id, p);
                queue.add(p);
            }
        }
    }

    /**
     * Empty means the invocation awaits an atomic lifecycle commit; ordinary contact replies
     * immediately.
     */
    public Optional<Reply> heartbeat(
            long id, BrokerControlProtocol.Heartbeat h, long now, ReplyMeta meta) {
        if (h.session() == null
                || h.recoveryId() == null
                || h.sequence() < 0
                || h.appliedOffset() < 0
                || h.appliedOffset() > image.appliedOffset())
            return Optional.of(
                    new Failure(
                            new ReplyMeta(
                                    QuorumError.INVALID_REQUEST,
                                    "Invalid heartbeat",
                                    meta.epoch(),
                                    meta.leaderId())));
        var view = image.brokers().get(h.session().brokerId());
        if (view == null || !view.registration().session().equals(h.session()))
            return Optional.of(heartbeatReply(h, meta, null));
        boolean fresh = heartbeatTracker.record(h.session(), h.sequence(), now);
        var pending = lifecycles.get(h.session().brokerId());
        if (h.recoveryId().equals(NO_RECOVERY)
                || registrations.containsKey(h.session().brokerId())
                || pending != null && pending.lifecycle.fenced())
            return Optional.of(heartbeatReply(h, meta, null));
        var recovery = recoveries.get(h.session().brokerId());
        if (recovery == null || !recovery.id.equals(h.recoveryId())) {
            if (!fresh || pending != null) return Optional.of(heartbeatReply(h, meta, null));
            recovery = new Recovery(h.recoveryId(), h.session(), image.appliedOffset());
            recoveries.put(h.session().brokerId(), recovery);
        }
        if (recovery.end <= image.appliedOffset())
            return Optional.of(heartbeatReply(h, meta, recovery));
        if (h.appliedOffset() >= recovery.target && h.recoveryComplete()) {
            if (pending == null) {
                if (!fresh) return Optional.of(heartbeatReply(h, meta, recovery));
                pending = new Pending(h, null, List.of());
                pending.recovery = recovery;
                pending.lifecycle =
                        new ClusterRecords.BrokerState(
                                h.session().brokerId(), h.session().brokerEpoch(), false);
                lifecycles.put(h.session().brokerId(), pending);
                queue.add(pending);
            }
            waiters.put(id, pending);
            return Optional.empty();
        }
        return Optional.of(heartbeatReply(h, meta, recovery));
    }

    private BrokerControlProtocol.HeartbeatReply heartbeatReply(
            BrokerControlProtocol.Heartbeat h, ReplyMeta meta, Recovery recovery) {
        var view = image.brokers().get(h.session().brokerId());
        var status =
                view == null
                        ? BrokerControlProtocol.SessionStatus.REGISTRATION_REQUIRED
                        : !view.registration().session().equals(h.session())
                                ? BrokerControlProtocol.SessionStatus.STALE_SESSION
                                : view.fenced()
                                        ? BrokerControlProtocol.SessionStatus.FENCED
                                        : BrokerControlProtocol.SessionStatus.ACTIVE;
        return new BrokerControlProtocol.HeartbeatReply(
                meta,
                status,
                view == null ? 0 : view.registration().session().brokerEpoch(),
                view == null ? image.appliedOffset() : view.stateOffset(),
                recovery == null ? image.appliedOffset() : recovery.target,
                h.recoveryId());
    }

    /** NONE reserves/coalesces the invocation; errors never reserve metadata capacity. */
    public QuorumError submit(long id, Request request) {
        if (image.metadataVersion() != 2) return QuorumError.NODE_UNAVAILABLE;
        Pending p;
        if (request instanceof BrokerControlProtocol.Register r) {
            try {
                new ClusterRecords.BrokerRegistration(
                        new ClusterRecords.Session(
                                r.brokerId(), r.storageId(), r.incarnationId(), 0),
                        r.endpoint(),
                        r.minVersion(),
                        r.maxVersion());
            } catch (IllegalArgumentException | NullPointerException e) {
                return QuorumError.INVALID_REQUEST;
            }
            if (r.expectedBrokerEpoch() < -1) return QuorumError.INVALID_REQUEST;
            if (r.minVersion() > 2 || r.maxVersion() < 2)
                return QuorumError.INCOMPATIBLE_METADATA_VERSION;
            var old = image.brokers().get(r.brokerId());
            if (old != null && !old.registration().session().storageId().equals(r.storageId()))
                return QuorumError.STORAGE_ID_MISMATCH;
            if (old != null
                    && old.registration().session().incarnationId().equals(r.incarnationId())) {
                if (!old.registration().endpoint().equals(r.endpoint())
                        || old.registration().minVersion() != r.minVersion()
                        || old.registration().maxVersion() != r.maxVersion())
                    return QuorumError.INVALID_REQUEST;
                p = new Pending(r, null, List.of());
                p.end = old.registration().session().brokerEpoch();
                waiters.put(id, p);
                return QuorumError.NONE;
            }
            if (r.expectedBrokerEpoch()
                    != (old == null ? -1 : old.registration().session().brokerEpoch()))
                return QuorumError.STALE_BROKER_EPOCH;
            if (lifecycles.containsKey(r.brokerId())) return QuorumError.BROKER_ID_IN_USE;
            if (old != null && !old.fenced()) return QuorumError.BROKER_ID_IN_USE;
            p = registrations.get(r.brokerId());
            if (p != null) {
                var reserved = (BrokerControlProtocol.Register) p.request;
                if (!reserved.storageId().equals(r.storageId())
                        || !reserved.incarnationId().equals(r.incarnationId())
                        || reserved.expectedBrokerEpoch() != r.expectedBrokerEpoch()
                        || !reserved.endpoint().equals(r.endpoint())
                        || reserved.minVersion() != r.minVersion()
                        || reserved.maxVersion() != r.maxVersion())
                    return QuorumError.BROKER_ID_IN_USE;
            }
            if (p == null) {
                if (old == null
                        && image.brokers().size()
                                        + registrations.keySet().stream()
                                                .filter(k -> !image.brokers().containsKey(k))
                                                .count()
                                >= limits.maxBrokers()) return QuorumError.OVERLOADED;
                p = new Pending(r, null, List.of());
                registrations.put(r.brokerId(), p);
                queue.add(p);
            }
        } else if (request instanceof BrokerControlProtocol.CreateTopic c) {
            try {
                new ClusterRecords.TopicRecord(new UUID(0, 1), c.name(), c.partitions());
            } catch (IllegalArgumentException | NullPointerException e) {
                return QuorumError.INVALID_REQUEST;
            }
            if (c.replicationFactor() != 1) return QuorumError.INVALID_REQUEST;
            var old =
                    image.topics().stream()
                            .filter(t -> t.name().equals(c.name()))
                            .findFirst()
                            .orElse(null);
            p = topics.get(c.name());
            if (old != null || p != null) {
                int count =
                        old != null
                                ? old.partitions()
                                : ((BrokerControlProtocol.CreateTopic) p.request).partitions();
                if (count != c.partitions()) return QuorumError.TOPIC_ALREADY_EXISTS;
                if (old != null) {
                    // Fresh barrier preserves the linearizable duplicate-create contract.
                    p = new Pending(c, old.id(), List.of());
                    queue.add(p);
                }
            } else {
                long total =
                        image.partitions().size()
                                + topics.values().stream()
                                        .mapToLong(t -> t.assignment.size())
                                        .sum();
                if (image.topics().size() + topics.size() >= limits.maxTopics()
                        || total + c.partitions() > limits.maxPartitions())
                    return QuorumError.OVERLOADED;
                var brokers =
                        image.brokers().entrySet().stream()
                                .filter(
                                        e ->
                                                !e.getValue().fenced()
                                                        && eligible.test(e.getKey())
                                                        && !lifecycles.containsKey(e.getKey()))
                                .map(Map.Entry::getKey)
                                .toList();
                if (brokers.isEmpty()) return QuorumError.NO_ELIGIBLE_BROKER;
                var loads = new HashMap<Integer, Integer>();
                image.partitions()
                        .values()
                        .forEach(a -> loads.merge(a.leaderId(), 1, Integer::sum));
                topics.values()
                        .forEach(t -> t.assignment.forEach(b -> loads.merge(b, 1, Integer::sum)));
                p = new Pending(c, UUID.randomUUID(), assign(brokers, loads, c.partitions()));
                if (storageSize(entries(p, 0, 0)) > batchBytes) return QuorumError.BATCH_TOO_LARGE;
                topics.put(c.name(), p);
                queue.add(p);
            }
        } else return QuorumError.UNSUPPORTED_OPERATION;
        waiters.put(id, p);
        return QuorumError.NONE;
    }

    /** Caller serializes drains with append completion; one command remains one whole batch. */
    public Optional<QuorumBatch> drainNext(long epoch, long baseOffset) {
        if (queue.peek() != null
                && queue.peek().lifecycle != null
                && image.appliedOffset() < baseOffset)
            return Optional
                    .empty(); // Capture every preceding assignment before advancing its epochs.
        var p = queue.poll();
        if (p == null) return Optional.empty();
        var batch = new QuorumBatch(baseOffset, entries(p, epoch, baseOffset));
        p.end = batch.nextOffset();
        if (p.recovery != null) p.recovery.end = p.end;
        return Optional.of(batch);
    }

    private List<QuorumEntry> entries(Pending p, long epoch, long base) {
        if (p.lifecycle != null) {
            var entries = new ArrayList<QuorumEntry>();
            entries.add(new QuorumEntry.BrokerState(epoch, p.lifecycle));
            if (!p.lifecycle.fenced())
                image.partitions().values().stream()
                        .filter(a -> a.leaderId() == p.lifecycle.brokerId())
                        .forEach(
                                a ->
                                        entries.add(
                                                new QuorumEntry.PartitionRecord(
                                                        epoch,
                                                        new ClusterRecords.PartitionRecord(
                                                                a.topicId(),
                                                                a.partitionId(),
                                                                a.replicas(),
                                                                a.leaderId(),
                                                                Math.addExact(a.leaderEpoch(), 1),
                                                                Math.addExact(
                                                                        a.partitionEpoch(), 1)))));
            return List.copyOf(entries);
        }
        if (p.request instanceof BrokerControlProtocol.Register r)
            return List.of(
                    new QuorumEntry.BrokerRegistration(
                            epoch,
                            new ClusterRecords.BrokerRegistration(
                                    new ClusterRecords.Session(
                                            r.brokerId(),
                                            r.storageId(),
                                            r.incarnationId(),
                                            base + 1),
                                    r.endpoint(),
                                    r.minVersion(),
                                    r.maxVersion())));
        var c = (BrokerControlProtocol.CreateTopic) p.request;
        if (p.assignment.isEmpty()) return List.of(new QuorumEntry.ReadBarrier(epoch));
        var entries = new ArrayList<QuorumEntry>();
        entries.add(
                new QuorumEntry.TopicRecord(
                        epoch,
                        new ClusterRecords.TopicRecord(p.topicId, c.name(), c.partitions())));
        for (int i = 0; i < c.partitions(); i++)
            entries.add(
                    new QuorumEntry.PartitionRecord(
                            epoch,
                            new ClusterRecords.PartitionRecord(
                                    p.topicId,
                                    i,
                                    List.of(p.assignment.get(i)),
                                    p.assignment.get(i),
                                    0,
                                    0)));
        return List.copyOf(entries);
    }

    public List<Completion> completions(ReplyMeta meta) {
        var done = new ArrayList<Completion>();
        var iterator = waiters.entrySet().iterator();
        while (iterator.hasNext()) {
            var w = iterator.next();
            var p = w.getValue();
            if (p.end > image.appliedOffset()) continue;
            Reply reply;
            if (p.request instanceof BrokerControlProtocol.Register r) {
                var registration = image.brokers().get(r.brokerId()).registration();
                reply =
                        new BrokerControlProtocol.RegisterReply(
                                meta,
                                registration.session(),
                                registration.session().brokerEpoch(),
                                image.appliedOffset());
            } else if (p.request instanceof BrokerControlProtocol.Heartbeat h)
                reply = heartbeatReply(h, meta, p.recovery);
            else reply = new BrokerControlProtocol.CreateTopicReply(meta, p.topicId, p.end);
            done.add(new Completion(w.getKey(), reply));
            iterator.remove();
        }
        return List.copyOf(done);
    }

    public void cancel(long invocationId) {
        waiters.remove(invocationId);
    }

    public void onLeadershipLost() {
        queue.clear();
        registrations.clear();
        topics.clear();
        waiters.clear();
        lifecycles.clear();
        recoveries.clear();
        heartbeatTracker = null;
    }

    public static long storageSize(List<QuorumEntry> entries) {
        return 30L + entries.stream().mapToLong(e -> 20L + QuorumEntryCodec.encodedSize(e)).sum();
    }

    public static List<Integer> assign(
            List<Integer> eligible, Map<Integer, Integer> initial, int count) {
        if (eligible.isEmpty() || count < 1)
            throw new IllegalArgumentException("Invalid assignment");
        var loads = new HashMap<>(initial);
        var result = new ArrayList<Integer>();
        for (int i = 0; i < count; i++) {
            int id =
                    eligible.stream()
                            .min(
                                    Comparator.comparingInt((Integer b) -> loads.getOrDefault(b, 0))
                                            .thenComparingInt(Integer::intValue))
                            .orElseThrow();
            result.add(id);
            loads.merge(id, 1, Math::addExact);
        }
        return List.copyOf(result);
    }
}

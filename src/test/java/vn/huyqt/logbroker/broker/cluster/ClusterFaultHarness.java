package vn.huyqt.logbroker.broker.cluster;

import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.client.ControllerClientTransport;
import vn.huyqt.logbroker.controller.log.QuorumEntry;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.SenderRole;
import vn.huyqt.logbroker.controller.support.*;
import vn.huyqt.logbroker.storage.LogConfig;
import vn.huyqt.logbroker.support.ManualScheduler;

/** Real quorum transitions, lifecycle, observer journal and codecs under controlled delivery. */
final class ClusterFaultHarness implements AutoCloseable {
    private final QuorumHarness quorum;
    private final ManualScheduler clock = new ManualScheduler();
    private final Random random;
    private final long seed;
    private final ArrayDeque<Runnable> network = new ArrayDeque<>(), disk = new ArrayDeque<>();
    private final ObserverStore[] stores = new ObserverStore[3];
    private final MetadataObserver[] observers = new MetadataObserver[3];
    private final BrokerControlClient[] controls = new BrokerControlClient[3];
    private final BrokerLifecycle[] lifecycles = new BrokerLifecycle[3];
    private final ServingGate[] gates = new ServingGate[3];
    private final List<Throwable> failures = new ArrayList<>();
    private final NavigableMap<Long, QuorumEntry> committed = new TreeMap<>();
    private final Set<Long> committedBoundaries = new HashSet<>(Set.of(0L));
    private final Map<String, UUID> acknowledged = new HashMap<>();
    private final ArrayDeque<String> trace = new ArrayDeque<>();
    private boolean isolatedBrokers;
    private int creations;
    private vn.huyqt.logbroker.client.ClusterClient dataClient;
    private vn.huyqt.logbroker.support.LoopbackTransport dataWire;
    private MetadataImage corruptObservation;

    ClusterFaultHarness(long seed) throws Exception {
        this.seed = seed; random = new Random(seed); quorum = QuorumHarness.threeNodes(seed, (short) 2);
        quorum.elect(0); synchronizeTime(); captureCommitted();
        var root = Files.createTempDirectory(Path.of("target"), "cluster-fault-");
        for (int index = 0; index < 3; index++) {
            int broker = 10 + index;
            var files = new FaultFiles(); var path = root.resolve("broker-" + broker);
            var identity = new BrokerIdentityStore.Identity(new UUID(0, 1), broker, new UUID(seed, broker + 1));
            var config = new BrokerClusterConfig(identity.clusterId(), broker, new Endpoint("localhost", 20000 + broker),
                    List.of(new Endpoint("localhost", 19090), new Endpoint("localhost", 19091), new Endpoint("localhost", 19092)),
                    Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(10), MetadataLimits.defaults());
            ObserverStore.format(path, identity.clusterId(), files, LogConfig.defaults());
            stores[index] = ObserverStore.open(path, identity.clusterId(), files, LogConfig.defaults(), MetadataLimits.defaults());
            controls[index] = new BrokerControlClient(config, new BridgeFactory(), clock, () -> 0);
            gates[index] = new ServingGate(new Session(broker, identity.storageId(), new UUID(seed, broker + 100), 0));
            observers[index] = new MetadataObserver(controls[index], stores[index], clock, disk::addLast,
                    ignored -> {}, failures::add);
            lifecycles[index] = new BrokerLifecycle(identity, controls[index], observers[index], clock, gates[index]);
            lifecycles[index].start();
        }
        healAndDrain();
        verifyLostProduceOnce();
    }
    private void verifyLostProduceOnce() {
        var address = new InetSocketAddress("localhost", 20001);
        dataWire = new vn.huyqt.logbroker.support.LoopbackTransport();
        dataClient = new vn.huyqt.logbroker.client.ClusterClient(
                vn.huyqt.logbroker.client.ClusterClientConfig.defaults(List.of(address)),
                vn.huyqt.logbroker.client.ClientConfig.defaults(address), unused -> dataWire, clock);
        long deadline = clock.nanoTime() + Duration.ofSeconds(30).toNanos();
        dataClient.refresh(deadline); clock.runDue();
        var topic = new UUID(seed, 999);
        var metadata = new vn.huyqt.logbroker.protocol.ClusterProtocol.MetadataReply(
                vn.huyqt.logbroker.protocol.Protocol.Error.none(), new UUID(0, 1), 1,
                List.of(new vn.huyqt.logbroker.protocol.ClusterProtocol.BrokerInfo(1, new Endpoint("localhost", 20001), 1, false)),
                List.of(new vn.huyqt.logbroker.protocol.ClusterProtocol.TopicInfo("unknown-test", topic,
                        List.of(new vn.huyqt.logbroker.protocol.ClusterProtocol.PartitionInfo(0,
                                vn.huyqt.logbroker.protocol.Protocol.Error.none(), List.of(1), 1, 1, 1)))));
        var frame = dataWire.sent().getLast();
        dataWire.reply(new vn.huyqt.logbroker.protocol.Protocol.ResponseFrame(frame.operation(), frame.version(), frame.requestId(), metadata));
        clock.runDue();
        var request = new vn.huyqt.logbroker.protocol.Protocol.Produce(vn.huyqt.logbroker.protocol.Protocol.AckMode.APPENDED, 30000,
                List.of(new vn.huyqt.logbroker.protocol.Protocol.ProduceEntry(new vn.huyqt.logbroker.protocol.Protocol.TopicPartition(topic, 0),
                        new vn.huyqt.logbroker.protocol.Protocol.Batch(List.of(new vn.huyqt.logbroker.storage.LogRecord(0, null, new byte[]{1}, List.of()))))));
        var result = dataClient.request(request, deadline); clock.runDue();
        dataWire.failConnection(new java.io.IOException("lost after send")); clock.runDue();
        if (((vn.huyqt.logbroker.protocol.ClusterProtocol.ProduceReply) result.join()).results().getFirst().outcome()
                != vn.huyqt.logbroker.protocol.ClusterProtocol.Outcome.UNKNOWN) throw new AssertionError("Lost Produce was classified safe");
    }
    void injectDuplicateRetryForTest() { dataWire.send(dataWire.sent().getLast()); }
    void injectUncommittedApplyForTest() {
        corruptObservation = new MetadataImage(committed.lastKey() + 2, List.of(), (short) 0, Map.of(), Map.of());
    }
    void injectStaleUnfenceForTest() {
        var previous = gates[0].session();
        var stale = new Session(previous.brokerId(), previous.storageId(), new UUID(seed, 888), previous.brokerEpoch() + 1);
        gates[0].bind(stale); gates[0].apply(stale.brokerEpoch() + 1, stale.brokerEpoch() + 1, false);
    }
    void injectPartialTopicForTest() {
        createIfReady(); healAndDrain();
        var topicOffset = committed.entrySet().stream().filter(e -> e.getValue() instanceof QuorumEntry.TopicRecord)
                .findFirst().orElseThrow().getKey() + 1;
        var topics = new ArrayList<vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated>();
        var partitions = new TreeMap<MetadataImage.PartitionKey, PartitionRecord>();
        for (var entry : committed.headMap(topicOffset, false).values()) {
            if (entry instanceof QuorumEntry.TopicRecord topic) topics.add(
                    new vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated(
                            topic.event().topicId(), topic.event().name(), topic.event().partitions()));
            if (entry instanceof QuorumEntry.PartitionRecord partition) partitions.put(
                    new MetadataImage.PartitionKey(partition.event().topicId(), partition.event().partitionId()), partition.event());
        }
        corruptObservation = new MetadataImage(topicOffset, topics, (short) 2, observers[0].image().brokers(), partitions);
    }
    void run(int steps) {
        try {
            for (int step = 0; step < steps; step++) {
                int node = random.nextInt(3), action = random.nextInt(16);
                trace.addLast(step + ":" + action + ":" + node); if (trace.size() > 80) trace.removeFirst();
                switch (action) {
                    case 0 -> { quorum.tick(Duration.ofMillis(20 + random.nextInt(80))); synchronizeTime(); }
                    case 1 -> { if (!network.isEmpty()) network.removeFirst().run(); }
                    case 2 -> { if (!network.isEmpty()) network.removeLast().run(); }
                    case 3 -> { if (!disk.isEmpty()) disk.removeFirst().run(); }
                    case 4 -> quorum.isolate(node);
                    case 5 -> { quorum.heal(); isolatedBrokers = false; }
                    case 6 -> isolatedBrokers = true;
                    case 7 -> { if (!network.isEmpty()) network.removeFirst(); }
                    case 8 -> { quorum.transport().reorder(); quorum.deliverOne(); }
                    case 9 -> { quorum.completeDisks(); quorum.deliverAll(); }
                    case 10 -> createIfReady();
                    case 11 -> {
                        var gate = gates[node]; long blocked = gate.blockedOffset();
                        if (blocked >= 0) {
                            boolean permitted = gate.canServe();
                            gate.apply(blocked, Math.max(0, blocked - 1), false);
                            if (!permitted && gate.canServe()) throw new AssertionError("Stale callback reopened fenced gate");
                        }
                    }
                    case 12 -> {
                        if (quorum.node(node) != null) quorum.powerLoss(node);
                        else quorum.restart(node);
                    }
                    case 13 -> {
                        if (quorum.node(node) != null) quorum.crash(node);
                        quorum.restart(node);
                    }
                    case 14 -> quorum.disk(node).failNextForce();
                    case 15 -> { if (random.nextBoolean()) quorum.pauseDisk(node); else quorum.resumeDisk(node); }
                }
                clock.runDue(); captureCommitted(); assertInvariants();
            }
        } catch (Throwable failure) { throw new AssertionError("seed=" + seed + " trace=" + trace, failure); }
    }
    private void createIfReady() {
        int leader = quorum.leader();
        if (leader < 0 || !quorum.node(leader).status().ready() || creations >= 8) return;
        String name = "topic-" + creations++;
        quorum.service(leader).request(new BrokerControlProtocol.CreateTopic(name, 3, (short) 1, 2000),
                quorum.now() + Duration.ofSeconds(2).toNanos()).whenComplete((reply, failure) -> {
            if (reply instanceof BrokerControlProtocol.CreateTopicReply created)
                acknowledged.put(name, created.topicId());
        });
    }
    private void synchronizeTime() {
        long difference = quorum.now() - clock.nanoTime();
        if (difference > 0) clock.advance(Duration.ofNanos(difference));
    }
    void healAndDrain() {
        quorum.heal(); isolatedBrokers = false;
        for (int node = 0; node < 3; node++) {
            quorum.disk(node).clearInjectedFailure();
            if (quorum.node(node) != null && quorum.node(node).status().role()
                    == vn.huyqt.logbroker.controller.consensus.QuorumStatus.Role.FAILED) quorum.crash(node);
            if (quorum.node(node) == null) quorum.restart(node);
            quorum.resumeDisk(node);
        }
        for (int round = 0; round < 250; round++) {
            quorum.tick(Duration.ofMillis(100)); synchronizeTime();
            for (int pass = 0; pass < 40; pass++) {
                clock.runDue(); quorum.settle(); captureCommitted();
                int pending = network.size() + disk.size();
                if (pending == 0) break;
                for (int index = 0; index < pending && !network.isEmpty(); index++) network.removeFirst().run();
                if (!disk.isEmpty()) disk.removeFirst().run();
            }
            assertInvariants();
            if (quorum.leader() >= 0 && quorum.node(quorum.leader()).status().ready()
                    && Arrays.stream(gates).allMatch(ServingGate::canServe)
                    && Arrays.stream(observers).allMatch(o -> o.image().appliedOffset() == quorum.image(quorum.leader()).appliedOffset())) return;
        }
        throw new AssertionError("Healthy suffix did not recover broker observers; seed=" + seed + " failures=" + failures
                + " quorum=" + java.util.stream.IntStream.range(0, 3).mapToObj(n -> quorum.node(n).status()).toList()
                + " brokers=" + Arrays.stream(lifecycles).map(l -> l.state() + ":" + l.session()).toList()
                + " offsets=" + Arrays.stream(observers).map(o -> o.image().appliedOffset()).toList());
    }
    private void captureCommitted() {
        for (int node = 0; node < 3; node++) if (quorum.node(node) != null)
            for (var batch : quorum.disk(node).batches()) if (batch.nextOffset() <= quorum.node(node).status().commit()) {
                committedBoundaries.add(batch.nextOffset());
                for (int index = 0; index < batch.entries().size(); index++) {
                    var previous = committed.putIfAbsent(batch.baseOffset() + index, batch.entries().get(index));
                    if (previous != null && !previous.equals(batch.entries().get(index))) throw new AssertionError("Committed entry changed");
                }
            }
    }
    void assertInvariants() {
        quorum.assertSafety();
        if (dataWire != null && dataWire.sent().stream().filter(frame -> frame.body()
                instanceof vn.huyqt.logbroker.protocol.ClusterProtocol.Produce).count() != 1)
            throw new AssertionError("UNKNOWN Produce was retried");
        if (!failures.isEmpty()) throw new AssertionError("Observer fatal failures: " + failures);
        if (network.size() > 256 || disk.size() > 6 || clock.pending() > 64) throw new AssertionError("Unbounded observer work");
        for (int index = 0; index < 3; index++) {
            var image = index == 0 && corruptObservation != null ? corruptObservation : observers[index].image();
            if (!committedBoundaries.contains(image.appliedOffset()) || !committedBoundaries.contains(stores[index].durableEnd()))
                throw new AssertionError("Observer published an interior or uncommitted batch boundary");
            if (image.appliedOffset() > stores[index].durableEnd()) throw new AssertionError("Apply before durable checkpoint");
            if (image.appliedOffset() > 0 && !committed.containsKey(image.appliedOffset() - 1)) throw new AssertionError("Uncommitted observer apply");
            var topics = new HashMap<UUID, vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated>();
            var partitions = new TreeMap<MetadataImage.PartitionKey, PartitionRecord>();
            var sessions = new HashMap<Integer, Session>(); var fenced = new HashMap<Integer, Boolean>();
            for (var entry : committed.headMap(image.appliedOffset(), false).values()) {
                if (entry instanceof QuorumEntry.TopicRecord topic) topics.put(topic.event().topicId(),
                        new vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated(topic.event().topicId(), topic.event().name(), topic.event().partitions()));
                if (entry instanceof QuorumEntry.PartitionRecord partition) partitions.put(
                        new MetadataImage.PartitionKey(partition.event().topicId(), partition.event().partitionId()), partition.event());
                if (entry instanceof QuorumEntry.BrokerRegistration registration) {
                    sessions.put(registration.event().session().brokerId(), registration.event().session());
                    fenced.put(registration.event().session().brokerId(), true);
                }
                if (entry instanceof QuorumEntry.BrokerState state) fenced.put(state.event().brokerId(), state.event().fenced());
            }
            if (!new HashSet<>(image.topics()).equals(new HashSet<>(topics.values())) || !image.partitions().equals(partitions))
                throw new AssertionError("Observer differs from independent committed-entry model / partial topic");
            if (gates[index].canServe() && (!gates[index].session().equals(sessions.get(10 + index))
                    || !Boolean.FALSE.equals(fenced.get(10 + index)))) throw new AssertionError("Stale serving grant");
            for (var topic : image.topics()) if (acknowledged.containsKey(topic.name()) && !acknowledged.get(topic.name()).equals(topic.id()))
                throw new AssertionError("Acknowledged topic UUID changed");
        }
    }
    @Override public void close() throws Exception {
        if (dataClient != null) dataClient.close();
        for (var lifecycle : lifecycles) if (lifecycle != null) lifecycle.stop();
        for (var control : controls) if (control != null) control.close();
        for (int pass = 0; pass < 30; pass++) { clock.runDue(); while (!disk.isEmpty()) disk.removeFirst().run(); }
        for (var store : stores) if (store != null) store.close(); clock.close();
    }
    private final class BridgeFactory implements ControllerClientTransport.Factory {
        public ControllerClientTransport create() { return new Bridge(); }
    }
    private final class Bridge implements ControllerClientTransport {
        private int target;
        private Consumer<Frame> receive;
        private Consumer<Throwable> failure;
        private boolean closed;
        public CompletableFuture<Void> connect(InetSocketAddress address, Consumer<Frame> receive, Consumer<Throwable> failure) {
            target = address.getPort() - 19090; this.receive = receive; this.failure = failure;
            return CompletableFuture.completedFuture(null);
        }
        public CompletableFuture<Void> send(Frame frame) {
            network.addLast(() -> {
                if (closed || isolatedBrokers) return;
                try {
                    var decoded = QuorumCodec.decode(QuorumCodec.encode(frame), QuorumCodec.WireLimits.observer(MetadataLimits.defaults()));
                    quorum.service(target).request((Request) decoded.message(), quorum.now() + Duration.ofSeconds(2).toNanos())
                            .whenComplete((reply, error) -> network.addLast(() -> {
                                if (closed || isolatedBrokers) return;
                                try {
                                    if (error != null) { failure.accept(error); return; }
                                    Reply body = reply;
                                    var identity = ControllerTestSupport.identity(target);
                                    if (body instanceof DescribeQuorumReply description) body = new BrokerControlProtocol.DescribeReply(
                                            description.meta(), description.status(), identity.voters(), identity.voterHash());
                                    receive.accept(QuorumCodec.decode(QuorumCodec.encode(new Frame((short) 2, SenderRole.VOTER,
                                            frame.operation(), true, identity.clusterId(), target, frame.requestId(), identity.voterHash(), body)),
                                            QuorumCodec.WireLimits.observer(MetadataLimits.defaults())));
                                } catch (Exception invalid) { failure.accept(invalid); }
                            }));
                } catch (Exception invalid) { failure.accept(invalid); }
            });
            return CompletableFuture.completedFuture(null);
        }
        public void close() { closed = true; }
    }
}

package vn.huyqt.logbroker.integration.support;

import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.cluster.BrokerIdentityStore;
import vn.huyqt.logbroker.client.*;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;
import vn.huyqt.logbroker.controller.support.ThreeControllerProcesses;
import vn.huyqt.logbroker.protocol.*;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Owns six production JVMs, strict roots and bounded client resources. */
public final class Phase4Processes implements AutoCloseable {
    private final Path root;
    private final ThreeControllerProcesses controllers;
    private final Process[] brokers = new Process[3];
    private final int[] ports = new int[3];
    private final DeadlineScheduler clock = DeadlineScheduler.system();
    private ClusterClient client;

    private Phase4Processes(Path root) throws Exception {
        this.root = root.toAbsolutePath().normalize();
        controllers = new ThreeControllerProcesses(this.root.resolve("controllers"), (short) 2);
    }

    public static Phase4Processes start(Path root) throws Exception {
        var cluster = new Phase4Processes(root);
        try {
            cluster.start();
            return cluster;
        } catch (Exception | AssertionError failure) {
            cluster.close();
            throw failure;
        }
    }

    private void start() throws Exception {
        controllers.startAll();
        controllers.awaitLeader(Duration.ofSeconds(20));
        String bootstrap =
                controllers.voters().stream()
                        .map(v -> v.host() + ":" + v.port())
                        .collect(java.util.stream.Collectors.joining(","));
        for (int id = 1; id <= 3; id++) {
            try (var socket = new ServerSocket(0)) {
                ports[id - 1] = socket.getLocalPort();
            }
            BrokerIdentityStore.format(data(id), controllers.clusterId(), id, new DurableFiles());
            Files.writeString(
                    config(id),
                    "cluster.id="
                            + controllers.clusterId()
                            + "\nbroker.id="
                            + id
                            + "\nadvertised.host=127.0.0.1\nadvertised.port="
                            + brokerPort(id)
                            + "\ncontroller.bootstrap.servers="
                            + bootstrap
                            + "\n");
            restartBroker(id);
            awaitRunning(id, Duration.ofSeconds(30));
        }
        var address = new InetSocketAddress("127.0.0.1", brokerPort(1));
        client =
                new ClusterClient(
                        new ClusterClientConfig(
                                List.of(address),
                                controllers.clusterId(),
                                32,
                                Duration.ofSeconds(30),
                                Duration.ofMillis(100),
                                16 * 1024 * 1024,
                                32),
                        ClientConfig.defaults(address),
                        unused -> new NettyClientTransport(ProtocolLimits.defaults()),
                        clock);
    }

    public ClusterClient client() {
        return client;
    }

    public ThreeControllerProcesses controllers() {
        return controllers;
    }

    public Path data(int id) {
        return root.resolve("broker-" + id);
    }

    private Path config(int id) {
        return root.resolve("broker-" + id + ".properties");
    }

    public int brokerPort(int id) {
        return ports[id - 1];
    }

    public void assertRejectedBrokerCli(boolean formatted) throws Exception {
        var duplicate = root.resolve(formatted ? "duplicate-root" : "unformatted-root");
        if (formatted)
            BrokerIdentityStore.format(duplicate, controllers.clusterId(), 1, new DurableFiles());
        var output = root.resolve(formatted ? "duplicate-cli.log" : "unformatted-cli.log");
        var child =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-Xms32m",
                                "-Xmx256m",
                                "-cp",
                                System.getProperty("java.class.path"),
                                "vn.huyqt.logbroker.broker.BrokerMain",
                                "--config",
                                config(1).toString(),
                                "--data",
                                duplicate.toString(),
                                "--port",
                                "0")
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            if (!child.waitFor(15, TimeUnit.SECONDS))
                throw new AssertionError(
                        "Rejected broker CLI stayed alive: " + Files.readString(output));
            if (child.exitValue() == 0)
                throw new AssertionError("Rejected broker CLI exited successfully");
            var text = Files.readString(output);
            if (formatted
                    && !text.contains("STORAGE_ID_MISMATCH")
                    && !text.contains("Broker storage identity changed"))
                throw new AssertionError(text);
            if (!formatted && Files.exists(duplicate.resolve("broker-identity.bin")))
                throw new AssertionError("Startup formatted an unformatted root");
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                if (!child.waitFor(10, TimeUnit.SECONDS))
                    throw new AssertionError("Rejected CLI child leaked");
            }
        }
    }

    public String runClientExample(String bootstrap, String topic) throws Exception {
        var output = root.resolve("client-example.log");
        var process =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-Xms32m",
                                "-Xmx256m",
                                "-cp",
                                System.getProperty("java.class.path"),
                                "vn.huyqt.logbroker.example.ClientExample",
                                bootstrap,
                                topic)
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            if (!process.waitFor(60, TimeUnit.SECONDS))
                throw new AssertionError("Client example timed out");
            var text = Files.readString(output);
            if (process.exitValue() != 0) throw new AssertionError(text);
            return text;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(10, TimeUnit.SECONDS))
                    throw new AssertionError("Example child leaked");
            }
        }
    }

    public String[] observerStatus(int id) throws Exception {
        return Files.readString(data(id).resolve("process-status.txt")).split(" ");
    }

    public void awaitObserverSnapshot(int id, Duration timeout) throws Exception {
        long end = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < end) {
            if (Long.parseLong(observerStatus(id)[3]) > 0) return;
            Thread.sleep(50);
        }
        throw new AssertionError("Observer did not install a snapshot");
    }

    public ClusterProtocol.MetadataReply metadataFrom(int id) throws Exception {
        var address = new InetSocketAddress("127.0.0.1", brokerPort(id));
        try (var local =
                new BrokerClient(
                        ClientConfig.defaults(address),
                        new NettyClientTransport(ProtocolLimits.defaults()),
                        clock)) {
            return (ClusterProtocol.MetadataReply)
                    local.request(
                                    new ClusterProtocol.Metadata(
                                            controllers.clusterId(), List.of()),
                                    deadline())
                            .get(35, TimeUnit.SECONDS);
        }
    }

    public ClusterProtocol.FetchReply fetchDirect(int id, UUID topic, int partition)
            throws Exception {
        var metadata = metadataFrom(id);
        var broker =
                metadata.brokers().stream().filter(b -> b.id() == id).findFirst().orElseThrow();
        var part =
                metadata.topics().stream()
                        .filter(t -> t.id().equals(topic))
                        .findFirst()
                        .orElseThrow()
                        .partitions()
                        .stream()
                        .filter(p -> p.partition() == partition)
                        .findFirst()
                        .orElseThrow();
        var route =
                new ClusterProtocol.Route(
                        new Protocol.TopicPartition(topic, partition),
                        id,
                        broker.brokerEpoch(),
                        part.leaderEpoch());
        var address = new InetSocketAddress("127.0.0.1", brokerPort(id));
        try (var local =
                new BrokerClient(
                        ClientConfig.defaults(address),
                        new NettyClientTransport(ProtocolLimits.defaults()),
                        clock)) {
            return (ClusterProtocol.FetchReply)
                    local.request(
                                    new ClusterProtocol.Fetch(
                                            controllers.clusterId(),
                                            1024,
                                            0,
                                            0,
                                            List.of(
                                                    new ClusterProtocol.FetchEntry(
                                                            route, 0, 1024))),
                                    deadline())
                            .get(35, TimeUnit.SECONDS);
        }
    }

    public void restartBroker(int id) throws Exception {
        if (brokers[id - 1] != null && brokers[id - 1].isAlive())
            throw new IllegalStateException("Broker already alive");
        Files.deleteIfExists(data(id).resolve("process-status.txt"));
        brokers[id - 1] =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                "-Xms32m",
                                "-Xmx256m",
                                "-cp",
                                System.getProperty("java.class.path"),
                                ClusterProcessMain.class.getName(),
                                config(id).toString(),
                                data(id).toString())
                        .redirectErrorStream(true)
                        .redirectOutput(
                                ProcessBuilder.Redirect.appendTo(
                                        root.resolve("broker-" + id + ".log").toFile()))
                        .start();
    }

    public void killBroker(int id) throws Exception {
        var process = brokers[id - 1];
        if (process == null) return;
        process.destroyForcibly();
        if (!process.waitFor(10, TimeUnit.SECONDS))
            throw new AssertionError("Owned broker did not terminate");
        brokers[id - 1] = null;
    }

    public void killController(int id) throws Exception {
        controllers.kill(id);
    }

    public boolean becomesRunning(int id, Duration timeout) throws Exception {
        long end = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < end) {
            var process = brokers[id - 1];
            if (process == null || !process.isAlive())
                throw new AssertionError("Broker exited; logs: " + root);
            var status = data(id).resolve("process-status.txt");
            if (Files.exists(status) && Files.readString(status).startsWith("true ")) return true;
            Thread.sleep(50);
        }
        return false;
    }

    public void awaitRunning(int id, Duration timeout) throws Exception {
        if (!becomesRunning(id, timeout))
            throw new AssertionError("Broker did not become RUNNING; logs: " + root);
    }

    private long deadline() {
        return clock.nanoTime() + Duration.ofSeconds(30).toNanos();
    }

    public UUID createTopic(String name, int partitions) throws Exception {
        long end = deadline();
        Protocol.Response response;
        do {
            response =
                    client.request(new Protocol.CreateTopic(name, partitions), end)
                            .get(35, TimeUnit.SECONDS);
            if (response instanceof Protocol.Failure failure
                    && failure.error().code() == ErrorCode.NO_ELIGIBLE_BROKER) {
                Thread.sleep(50);
            } else break;
        } while (clock.nanoTime() < end);
        if (!(response instanceof ClusterProtocol.CreateTopicReply reply))
            throw new AssertionError(response);
        if (reply.error().code() != ErrorCode.NONE) throw new AssertionError(reply);
        UUID topic = reply.topicId();
        do {
            var metadata = client.refresh(end).get(35, TimeUnit.SECONDS);
            var found = metadata.topics().stream().filter(t -> t.id().equals(topic)).findFirst();
            if (found.isPresent()
                    && found.get().partitions().size() == partitions
                    && found.get().partitions().stream()
                            .allMatch(p -> p.error().code() == ErrorCode.NONE)) return topic;
            Thread.sleep(50);
        } while (clock.nanoTime() < end);
        throw new AssertionError("Assignments did not become ready");
    }

    public int owner(UUID topic, int partition) throws Exception {
        return client.refresh(deadline()).get(35, TimeUnit.SECONDS).topics().stream()
                .filter(t -> t.id().equals(topic))
                .findFirst()
                .orElseThrow()
                .partitions()
                .stream()
                .filter(p -> p.partition() == partition)
                .findFirst()
                .orElseThrow()
                .leaderId();
    }

    public long produceFlushed(UUID topic, int partition, byte[] value) throws Exception {
        var tp = new Protocol.TopicPartition(topic, partition);
        var request =
                new Protocol.Produce(
                        Protocol.AckMode.FLUSHED,
                        30000,
                        List.of(
                                new Protocol.ProduceEntry(
                                        tp,
                                        new Protocol.Batch(
                                                List.of(
                                                        new LogRecord(
                                                                0, null, value, List.of()))))));
        var reply =
                (ClusterProtocol.ProduceReply)
                        client.request(request, deadline()).get(35, TimeUnit.SECONDS);
        var result = reply.results().getFirst();
        if (result.outcome() != ClusterProtocol.Outcome.SUCCESS) throw new AssertionError(result);
        return result.firstOffset();
    }

    public List<byte[]> fetchValues(UUID topic, int partition) throws Exception {
        var request =
                new Protocol.Fetch(
                        1024 * 1024,
                        0,
                        0,
                        List.of(
                                new Protocol.FetchEntry(
                                        new Protocol.TopicPartition(topic, partition),
                                        0,
                                        1024 * 1024)));
        var reply =
                (ClusterProtocol.FetchReply)
                        client.request(request, deadline()).get(35, TimeUnit.SECONDS);
        var result = reply.results().getFirst();
        if (result.error().code() != ErrorCode.NONE) throw new AssertionError(result);
        return result.batches().stream()
                .flatMap(batch -> batch.batch().records().stream())
                .map(LogRecord::value)
                .toList();
    }

    public void disconnectControllersFromBrokers() {
        controllers.blockBrokerControl(true);
    }

    public void heal() {
        controllers.blockBrokerControl(false);
        controllers.heal();
    }

    public void awaitControlIsolation(Duration duration) throws Exception {
        long initiallyBlocked = controllers.blockedBrokerFrames();
        long end = System.nanoTime() + duration.toNanos();
        while (System.nanoTime() < end) {
            if (!controllers.brokerControlBlocked())
                throw new AssertionError("Control isolation lost");
            for (var process : brokers)
                if (process != null && !process.isAlive())
                    throw new AssertionError("Broker exited in isolation");
            Thread.sleep(50);
        }
        if (controllers.blockedBrokerFrames() <= initiallyBlocked)
            throw new AssertionError("No broker control retries were observed during isolation");
    }

    public void awaitControlIsolationAfterMajorityLoss(Duration duration) throws Exception {
        long end = System.nanoTime() + duration.toNanos();
        while (System.nanoTime() < end) {
            for (int id = 1; id <= 3; id++)
                if (!becomesRunning(id, Duration.ofMillis(200)))
                    throw new AssertionError(
                            "Warmed broker stopped serving without a committed fence");
            Thread.sleep(50);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) client.close();
        clock.close();
        Exception failure = null;
        for (int id = 1; id <= 3; id++)
            try {
                killBroker(id);
            } catch (Exception error) {
                failure = error;
            }
        try {
            controllers.close();
        } catch (Exception error) {
            failure = error;
        }
        var retained =
                Path.of(
                        "target",
                        "cluster-process-logs",
                        root.getFileName() + "-" + controllers.clusterId());
        Files.createDirectories(retained);
        for (int id = 1; id <= 3; id++) {
            var log = root.resolve("broker-" + id + ".log");
            if (Files.exists(log))
                Files.copy(
                        log,
                        retained.resolve(log.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
        }
        if (failure != null) throw failure;
    }
}

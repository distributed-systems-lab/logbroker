package vn.huyqt.logbroker.controller;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;

/**
 * Process entry point: {@code --config FILE} starts one controller from a properties file and runs
 * it until JVM shutdown. The data root must already be formatted with {@code ControllerCli format};
 * see {@code docs/controller-configuration.md} for properties and the example cluster.
 */
public final class ControllerMain {
    private ControllerMain() {}

    /** Parsed properties: the data root and the validated controller configuration. */
    public record Settings(Path data, ControllerConfig config) {}

    public static void main(String[] args) {
        int code = run(args, System.out, System.err);
        if (code != 0) System.exit(code);
    }

    /**
     * Opens and starts the node, prints a {@code STARTED} line, then blocks until the shutdown hook
     * has closed the node or the calling thread is interrupted. The node is closed again on the way
     * out; a second close after a successful one does nothing.
     *
     * @return 0 after an orderly stop, 1 after any startup or shutdown error (reported on {@code
     *     err})
     */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            var options = ControllerOptions.parse(args, 0, Set.of("config"));
            var settings = settings(Path.of(ControllerOptions.required(options, "config")));
            var node = ControllerNode.open(settings.data(), settings.config(), new DurableFiles());
            var stop = new CountDownLatch(1);
            var hook =
                    new Thread(
                            () -> {
                                try {
                                    node.close();
                                } catch (IOException error) {
                                    err.println("ERROR shutdown: " + error.getMessage());
                                } finally {
                                    stop.countDown();
                                }
                            },
                            "controller-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                var bound = node.start();
                out.println(
                        "STARTED node="
                                + settings.config().identity().nodeId()
                                + " address="
                                + bound
                                + " data="
                                + settings.data().toAbsolutePath());
                out.flush();
                stop.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                } catch (IllegalStateException shuttingDown) {
                    // The JVM is already shutting down, so the hook cannot be removed; close below
                    // is then a
                    // no-op, or a retry if the hook's close failed.
                }
                node.close();
            }
            return 0;
        } catch (Exception error) {
            err.println("ERROR " + error.getMessage());
            return 1;
        }
    }

    /**
     * Reads a controller properties file. {@code cluster.id}, {@code node.id}, {@code voters} and
     * {@code data.dir} are required; other keys override builder defaults, and {@code *.ms} keys
     * are milliseconds. A relative {@code data.dir} is resolved against the working directory.
     *
     * @throws IllegalArgumentException if a required key is missing, a key is unknown, a value does
     *     not parse, or the resulting configuration is invalid
     */
    public static Settings settings(Path path) throws IOException {
        var properties = new Properties();
        try (var input = Files.newBufferedReader(path)) {
            properties.load(input);
        }
        var values = new HashMap<String, String>();
        for (String key : properties.stringPropertyNames())
            values.put(key, properties.getProperty(key));
        String cluster = remove(values, "cluster.id"), voters = remove(values, "voters");
        int node = Integer.parseInt(remove(values, "node.id"));
        Path data = Path.of(remove(values, "data.dir"));
        short metadataVersion = Short.parseShort(values.getOrDefault("metadata.version", "2"));
        values.remove("metadata.version");
        var builder =
                ControllerConfig.builder(
                        ControllerOptions.identity(cluster, node, voters, metadataVersion));
        if (values.containsKey("broker.session.timeout.ms"))
            builder.brokerSessionTimeout(
                    java.time.Duration.ofMillis(
                            Long.parseLong(values.remove("broker.session.timeout.ms"))));
        var log = vn.huyqt.logbroker.storage.LogConfig.defaults();
        builder.logConfig(
                new vn.huyqt.logbroker.storage.LogConfig(
                        Long.parseLong(
                                values.getOrDefault(
                                        "log.segment.bytes", Long.toString(log.segmentBytes()))),
                        Integer.parseInt(
                                values.getOrDefault(
                                        "log.max.batch.bytes",
                                        Integer.toString(log.maxBatchBytes()))),
                        Integer.parseInt(
                                values.getOrDefault(
                                        "log.index.interval.bytes",
                                        Integer.toString(log.indexIntervalBytes())))));
        values.remove("log.segment.bytes");
        values.remove("log.max.batch.bytes");
        values.remove("log.index.interval.bytes");
        for (var entry : values.entrySet()) {
            String key = entry.getKey(), value = entry.getValue();
            switch (key) {
                case "fetch.idle.wait.ms" -> builder.fetchIdleWait(ms(value));
                case "rpc.timeout.ms" -> builder.rpcTimeout(ms(value));
                case "election.min.ms" -> builder.electionMin(ms(value));
                case "election.max.ms" -> builder.electionMax(ms(value));
                case "leader.contact.timeout.ms" -> builder.leaderContactTimeout(ms(value));
                case "admin.timeout.ms" -> builder.adminTimeout(ms(value));
                case "shutdown.timeout.ms" -> builder.shutdownTimeout(ms(value));
                case "max.frame.bytes" -> builder.maxFrameBytes(Integer.parseInt(value));
                case "fetch.max.bytes" -> builder.fetchMaxBytes(Integer.parseInt(value));
                case "snapshot.chunk.bytes" -> builder.snapshotChunkBytes(Integer.parseInt(value));
                case "snapshot.max.bytes" -> builder.snapshotMaxBytes(Integer.parseInt(value));
                case "snapshot.trigger.bytes" ->
                        builder.snapshotTriggerBytes(Long.parseLong(value));
                case "max.pending.requests" -> builder.maxPendingRequests(Integer.parseInt(value));
                case "event.queue.capacity" -> builder.eventQueueCapacity(Integer.parseInt(value));
                case "disk.queue.capacity" -> builder.diskQueueCapacity(Integer.parseInt(value));
                case "inbound.bytes" -> builder.inboundBytes(Long.parseLong(value));
                case "outbound.bytes" -> builder.outboundBytes(Long.parseLong(value));
                case "max.topics" -> builder.maxTopics(Integer.parseInt(value));
                case "max.partitions" -> builder.maxPartitions(Integer.parseInt(value));
                default ->
                        throw new IllegalArgumentException("Unknown controller property: " + key);
            }
        }
        return new Settings(data, builder.build());
    }

    private static Duration ms(String value) {
        return Duration.ofMillis(Long.parseLong(value));
    }

    private static String remove(Map<String, String> values, String key) {
        String value = values.remove(key);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Missing property: " + key);
        return value;
    }
}

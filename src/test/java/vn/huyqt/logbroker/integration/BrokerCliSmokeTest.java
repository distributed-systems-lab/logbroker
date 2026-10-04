package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerCliSmokeTest {
    @Test
    @org.junit.jupiter.api.condition.DisabledOnOs(
            value = org.junit.jupiter.api.condition.OS.WINDOWS,
            disabledReason = "Strict broker identity acceptance requires Linux/ext4")
    void productionCliRejectsUnformattedRootAndDuplicateBrokerIdentity() throws Exception {
        try (var cluster =
                vn.huyqt.logbroker.integration.support.Phase4Processes.start(directory)) {
            cluster.assertRejectedBrokerCli(false);
            cluster.assertRejectedBrokerCli(true);
        }
    }

    @Test
    @org.junit.jupiter.api.condition.DisabledOnOs(
            value = org.junit.jupiter.api.condition.OS.WINDOWS,
            disabledReason = "Strict cluster CLI acceptance requires Linux/ext4")
    void productionExampleRoutesSixPartitionsFromOneBootstrap() throws Exception {
        try (var cluster =
                vn.huyqt.logbroker.integration.support.Phase4Processes.start(directory)) {
            var output = cluster.runClientExample("127.0.0.1:" + cluster.brokerPort(1), "demo");
            assertTrue(output.contains("SUCCESS records=6"), output);
            assertTrue(output.contains("brokers=3"), output);
        }
    }

    @TempDir Path directory;

    @Test
    void standaloneBrokerAndClientCanRunThenRestartWithSameData() throws Exception {
        runOnce();
        runOnce();
    }

    private void runOnce() throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        var broker =
                new ProcessBuilder(
                                java,
                                "-cp",
                                classpath,
                                "vn.huyqt.logbroker.broker.LegacyBrokerMain",
                                "--data",
                                directory.toString(),
                                "--port",
                                "0")
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .start();
        try {
            var ready =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new BufferedReader(
                                                    new InputStreamReader(broker.getInputStream()))
                                            .readLine();
                                } catch (Exception error) {
                                    throw new RuntimeException(error);
                                }
                            });
            String line = ready.get(10, TimeUnit.SECONDS);
            assertNotNull(line);
            assertTrue(line.startsWith("READY "), line);
            int port = Integer.parseInt(line.substring(6));
            var client =
                    new ProcessBuilder(
                                    java,
                                    "-cp",
                                    classpath,
                                    "vn.huyqt.logbroker.example.LegacyClientExample",
                                    "127.0.0.1",
                                    Integer.toString(port),
                                    "demo")
                            .redirectErrorStream(true)
                            .start();
            try {
                assertTrue(client.waitFor(10, TimeUnit.SECONDS));
                String output = new String(client.getInputStream().readAllBytes());
                assertEquals(0, client.exitValue(), output);
                assertTrue(output.contains("SUCCESS records=1"), output);
            } finally {
                client.destroyForcibly();
            }
        } finally {
            broker.destroyForcibly();
            assertTrue(broker.waitFor(10, TimeUnit.SECONDS));
        }
    }
}

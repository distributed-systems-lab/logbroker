package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.controller.client.ControllerCli;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.support.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

class ControllerCliTest {
    @TempDir Path root;

    @Test
    void generateIdAndRejectUnknownDuplicateMissingOptions() {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();
        assertEquals(
                0,
                ControllerCli.run(
                        new String[] {"generate-cluster-id"},
                        new PrintStream(out),
                        new PrintStream(err)));
        assertNotNull(UUID.fromString(out.toString().trim()));
        for (var args :
                List.of(
                        new String[] {"wat"},
                        new String[] {"metadata", "--wat", "1"},
                        new String[] {"metadata", "--cluster", "x", "--cluster", "y"},
                        new String[] {"format", "--data"}))
            assertNotEquals(0, ControllerCli.run(args, new PrintStream(out), new PrintStream(err)));
    }

    @Test
    void refusesExistingStorageWithoutTouchingIdentity() throws Exception {
        var identity = ControllerTestSupport.identity(0);
        QuorumStateStore.format(root, identity, new FaultFiles());
        byte[] original = Files.readAllBytes(root.resolve("identity.bin"));
        var sink = new PrintStream(new ByteArrayOutputStream());
        assertNotEquals(
                0,
                ControllerCli.run(
                        new String[] {
                            "format",
                            "--data",
                            root.toString(),
                            "--node",
                            "0",
                            "--cluster",
                            identity.clusterId().toString(),
                            "--voters",
                            "0@localhost:19090,1@localhost:19091,2@localhost:19092"
                        },
                        sink,
                        sink));
        assertArrayEquals(original, Files.readAllBytes(root.resolve("identity.bin")));
    }

    @Test
    void configRejectsUnknownPropertiesAndUsesAllThreeVoters() throws Exception {
        Path file = root.resolve("controller.properties");
        Files.writeString(
                file,
                "cluster.id=00000000-0000-0000-0000-000000000001\nnode.id=1\nvoters=0@localhost:19090,1@localhost:19091,2@localhost:19092\ndata.dir=data\n");
        var settings = ControllerMain.settings(file);
        assertEquals(1, settings.config().identity().nodeId());
        assertEquals(3, settings.config().identity().voters().size());
        assertEquals(2, settings.config().identity().metadataVersion());
        Files.writeString(file, "unknown.option=yes\n", StandardOpenOption.APPEND);
        assertThrows(IllegalArgumentException.class, () -> ControllerMain.settings(file));
    }
}

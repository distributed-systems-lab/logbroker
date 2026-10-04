package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;
import vn.huyqt.logbroker.controller.support.FaultFiles;

class BrokerIdentityStoreTest {
    @TempDir Path temporary;

    @Test
    void corruptionOrLegacyVersionCannotOpenAsClusterStorage() throws Exception {
        var root = temporary.resolve("broker");
        var files = new FaultFiles();
        var cluster = new UUID(0, 1);
        BrokerIdentityStore.format(root, cluster, 7, files);
        var path = root.resolve("broker-identity.bin");
        byte[] original = Files.readAllBytes(path);
        assertEquals(50, original.length);
        assertEquals(0x42494432, java.nio.ByteBuffer.wrap(original).getInt());
        for (int position : List.of(4, 6, 10, 49)) {
            byte[] broken = original.clone();
            broken[position] ^= 1;
            Files.write(path, broken);
            assertThrows(
                    IOException.class, () -> BrokerIdentityStore.open(root, cluster, 7, files));
        }
        Files.write(path, original);
        try (var opened = BrokerIdentityStore.open(root, cluster, 7, files)) {
            assertEquals(7, opened.identity().brokerId());
        }
    }

    @Test
    void openingUnformattedRootFailsWithoutCreatingIt() {
        var root = temporary.resolve("missing");
        assertThrows(
                IOException.class,
                () -> BrokerIdentityStore.open(root, new UUID(0, 1), 7, new DurableFiles()));
        assertFalse(Files.exists(root));
    }

    @Test
    void formattedIdentityIsStableAndLockExcludesAnotherOwner() throws Exception {
        var root = temporary.resolve("broker");
        var files = new FaultFiles();
        var cluster = new UUID(0, 1);
        var identity = BrokerIdentityStore.format(root, cluster, 7, files);
        assertNotEquals(new UUID(0, 0), identity.storageId());
        try (var store = BrokerIdentityStore.open(root, cluster, 7, files)) {
            assertEquals(identity, store.identity());
            assertThrows(
                    IOException.class, () -> BrokerIdentityStore.open(root, cluster, 7, files));
        }
        try (var store = BrokerIdentityStore.open(root, cluster, 7, files)) {
            assertEquals(identity, store.identity());
        }
        assertThrows(
                IOException.class, () -> BrokerIdentityStore.open(root, new UUID(0, 2), 7, files));
        assertThrows(IOException.class, () -> BrokerIdentityStore.open(root, cluster, 8, files));
    }

    @Test
    void nonemptyRootIsUnchangedAndMissingComponentsFailClosed() throws Exception {
        var root = temporary.resolve("broker");
        var files = new FaultFiles();
        var cluster = new UUID(0, 1);
        BrokerIdentityStore.format(root, cluster, 7, files);
        byte[] original = Files.readAllBytes(root.resolve("broker-identity.bin"));
        assertThrows(IOException.class, () -> BrokerIdentityStore.format(root, cluster, 7, files));
        assertArrayEquals(original, Files.readAllBytes(root.resolve("broker-identity.bin")));
        Files.delete(root.resolve("partition-inventory.journal"));
        assertThrows(IOException.class, () -> BrokerIdentityStore.open(root, cluster, 7, files));
    }

    @Test
    void formatForceFailureNeverProducesOpenableRoot() {
        var root = temporary.resolve("broker");
        var files = new FaultFiles();
        files.failAfter(2);
        assertThrows(
                IOException.class,
                () -> BrokerIdentityStore.format(root, new UUID(0, 1), 7, files));
        assertThrows(
                IOException.class,
                () -> BrokerIdentityStore.open(root, new UUID(0, 1), 7, new FaultFiles()));
    }
}

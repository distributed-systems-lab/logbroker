package vn.huyqt.logbroker.controller;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.support.*;

class ControllerLifecycleTest {
  @TempDir Path root;

  @Test
  void snapshotTriggerCountsStorageBatchBytes() {
    var entry = new vn.huyqt.logbroker.controller.log.QuorumEntry.LeaderChange(1, 0);
    var batch = new vn.huyqt.logbroker.controller.log.QuorumBatch(0, java.util.List.of(entry));
    var record =
        new vn.huyqt.logbroker.storage.LogRecord(
            0,
            null,
            vn.huyqt.logbroker.controller.log.QuorumEntryCodec.encode(entry),
            java.util.List.of());
    assertEquals(
        30L + vn.huyqt.logbroker.storage.RecordPayloadCodec.encodedSize(java.util.List.of(record)),
        ControllerNode.size(batch));
  }

  @Test
  void failedDurabilityStopsParticipationAndFailsNewRequests() throws Exception {
    var files = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, files);
    var config =
        ControllerConfig.builder(identity)
            .fetchIdleWait(Duration.ofMillis(10))
            .rpcTimeout(Duration.ofMillis(50))
            .electionMin(Duration.ofMillis(100))
            .electionMax(Duration.ofMillis(200))
            .build();
    try (var node = ControllerNode.open(root, config, files)) {
      files.failAfter(1);
      node.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
      while (node.status().join().role()
              != vn.huyqt.logbroker.controller.consensus.QuorumStatus.Role.FAILED
          && System.nanoTime() < deadline) Thread.sleep(10);
      assertEquals(
          vn.huyqt.logbroker.controller.consensus.QuorumStatus.Role.FAILED,
          node.status().join().role());
      var error =
          assertThrows(
              CompletionException.class,
              () ->
                  node.service()
                      .readMetadata(System.nanoTime() + TimeUnit.SECONDS.toNanos(1))
                      .join());
      assertEquals(
          vn.huyqt.logbroker.controller.protocol.QuorumError.NODE_UNAVAILABLE,
          ((ControllerService.ServiceException) error.getCause()).meta().error());
      files.clearFailure();
    }
  }

  @Test
  void openRecoversWithoutBindingAndConcurrentRootOpenFails() throws Exception {
    var files = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, files);
    try (var node = ControllerNode.open(root, ControllerConfig.defaults(identity), files)) {
      assertFalse(node.status().get().ready());
      assertThrows(
          IOException.class,
          () -> ControllerNode.open(root, ControllerConfig.defaults(identity), files));
      assertFalse(node.isStarted());
      assertEquals(
          vn.huyqt.logbroker.controller.consensus.QuorumStatus.Role.UNATTACHED,
          node.status().get().role());
    }
    try (var state = QuorumStateStore.open(root, identity, files)) {
      assertEquals(identity, state.identity());
    }
  }

  @Test
  void blockedDiskShutdownTimesOutWithoutReleasingStorageLock() throws Exception {
    var reached = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    class BlockingFiles extends DurableFiles {
      volatile boolean block;

      @Override
      public void syncDirectory(Path path) {}

      @Override
      public void forceFile(Path path) throws IOException {
        if (block && path.getFileName().toString().equals("quorum-state.journal")) {
          reached.countDown();
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          }
        }
        super.forceFile(path);
      }
    }
    var files = new BlockingFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, files);
    var config =
        ControllerConfig.builder(identity)
            .fetchIdleWait(Duration.ofMillis(10))
            .rpcTimeout(Duration.ofMillis(50))
            .electionMin(Duration.ofMillis(100))
            .electionMax(Duration.ofMillis(200))
            .shutdownTimeout(Duration.ofMillis(100))
            .build();
    var node = ControllerNode.open(root, config, files);
    try {
      files.block = true;
      node.start();
      assertTrue(reached.await(3, TimeUnit.SECONDS));
      assertThrows(IOException.class, node::close);
      assertThrows(IOException.class, () -> QuorumStateStore.open(root, identity, files));
    } finally {
      release.countDown();
      long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (true)
        try {
          node.close();
          break;
        } catch (IOException timedOut) {
          if (System.nanoTime() >= cleanupDeadline) throw timedOut;
          Thread.sleep(20);
        }
    }
    try (var state = QuorumStateStore.open(root, identity, files)) {
      assertEquals(identity, state.identity());
    }
  }
}

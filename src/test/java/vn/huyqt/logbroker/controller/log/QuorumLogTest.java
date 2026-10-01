package vn.huyqt.logbroker.controller.log;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.storage.LogConfig;

class QuorumLogTest {
  @TempDir Path root;

  @Test
  void replicationPreservesBatchBoundariesAndRequiresContiguousEpochs() throws Exception {
    var io = new FaultFiles();
    try (var leader = QuorumLog.open(root.resolve("leader"), LogConfig.defaults(), 0, 0, true, io);
        var follower =
            QuorumLog.open(root.resolve("follower"), LogConfig.defaults(), 0, 0, true, io)) {
      var batch =
          leader.append(
              2, List.of(new QuorumEntry.LeaderChange(2, 0), new QuorumEntry.ReadBarrier(2)));
      assertEquals(0, leader.durableEnd());
      follower.appendReplica(batch);
      assertEquals(batch, follower.read(0, 1024).getFirst());
      assertEquals(2, follower.flush());
      assertThrows(IllegalArgumentException.class, () -> follower.appendReplica(batch));
      assertThrows(
          IllegalArgumentException.class,
          () -> follower.append(1, List.of(new QuorumEntry.ReadBarrier(1))));
      assertThrows(IllegalArgumentException.class, () -> follower.truncate(0, 2));
      assertThrows(IllegalArgumentException.class, () -> follower.truncate(1, 0));
    }
  }

  @Test
  void truncationRebuildsEpochLookup() throws Exception {
    try (var log = QuorumLog.open(root, LogConfig.defaults(), 0, 0, true, new FaultFiles())) {
      log.append(1, List.of(new QuorumEntry.ReadBarrier(1)));
      log.append(3, List.of(new QuorumEntry.ReadBarrier(3)));
      log.flush();
      log.truncate(1, 1);
      assertEquals(1, log.end());
      assertEquals(1, log.epochs().positionAt(1).lastEpoch());
      assertThrows(IllegalArgumentException.class, () -> log.epochs().positionAt(2));
    }
  }
}

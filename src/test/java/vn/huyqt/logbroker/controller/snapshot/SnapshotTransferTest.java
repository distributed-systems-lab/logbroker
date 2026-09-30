package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.support.*;

class SnapshotTransferTest {
  @TempDir Path root;

  @Test
  void duplicateChunkIsComparedWithoutAppendingAndPartialRestartIsDiscarded() throws Exception {
    var io = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, io);
    var id = new SnapshotId(9, 3, new UUID(0, 9));
    var image = new MetadataImage(9, List.of());
    try (var state = QuorumStateStore.open(root, identity, io)) {
      var store = new SnapshotStore(root, identity, io, state, 65536);
      var bytes = store.encode(id, image);
      store.beginDownload(id);
      store.writeChunk(id, 0, Arrays.copyOfRange(bytes, 0, 50));
      store.writeChunk(id, 0, Arrays.copyOfRange(bytes, 0, 50));
      assertEquals(50, store.downloadedBytes());
      assertThrows(java.io.IOException.class, () -> store.writeChunk(id, 60, new byte[] {1}));
      store.cancelDownload();
      store.beginDownload(id);
      store.writeChunk(id, 0, bytes);
      assertEquals(image, store.finishDownload(id, bytes.length));
      assertEquals(image, store.load(id));
      store.beginDownload(new SnapshotId(10, 3, new UUID(0, 10)));
      store.writeChunk(new SnapshotId(10, 3, new UUID(0, 10)), 0, new byte[] {1, 2, 3});
      store.close();
    }
    try (var state = QuorumStateStore.open(root, identity, io);
        var store = new SnapshotStore(root, identity, io, state, 65536)) {
      store.beginDownload(new SnapshotId(10, 3, new UUID(0, 10)));
      assertEquals(0, store.downloadedBytes());
    }
  }

  @Test
  void changedIdentityConflictingDuplicateAndBadWholeChecksumCannotPublish() throws Exception {
    var io = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, io);
    try (var state = QuorumStateStore.open(root, identity, io);
        var store = new SnapshotStore(root, identity, io, state, 65536)) {
      var id = new SnapshotId(9, 3, new UUID(0, 9));
      var bytes = store.encode(id, new MetadataImage(9, List.of()));
      store.beginDownload(id);
      store.writeChunk(id, 0, bytes);
      assertThrows(
          java.io.IOException.class,
          () -> store.writeChunk(new SnapshotId(9, 3, new UUID(0, 99)), 0, bytes));
      byte[] bad = bytes.clone();
      bad[0] ^= 1;
      assertThrows(java.io.IOException.class, () -> store.writeChunk(id, 0, bad));
      store.cancelDownload();
      store.beginDownload(id);
      byte[] corrupt = bytes.clone();
      corrupt[corrupt.length - 1] ^= 1;
      store.writeChunk(id, 0, corrupt);
      assertThrows(java.io.IOException.class, () -> store.finishDownload(id, bytes.length));
      assertFalse(Files.exists(store.path(id)));
    }
  }

  @Test
  void uploadPinsSurviveRetentionRotationAndLimitConcurrentTransfers() throws Exception {
    var io = new FaultFiles();
    var identity = ControllerTestSupport.identity(0);
    QuorumStateStore.format(root, identity, io);
    try (var state = QuorumStateStore.open(root, identity, io);
        var store = new SnapshotStore(root, identity, io, state, 65536)) {
      var first = store.create(new MetadataImage(1, List.of()), 1);
      store.readUpload(1, first, 0, 10);
      store.readUpload(2, first, 0, 10);
      assertEquals(2, store.activeUploads());
      assertThrows(SnapshotStore.Unavailable.class, () -> store.readUpload(3, first, 0, 10));
      store.create(new MetadataImage(2, List.of()), 1);
      store.create(new MetadataImage(3, List.of()), 1);
      store.releaseObsolete();
      assertTrue(Files.exists(store.path(first)));
      var one = store.readUpload(1, first, 10, 256);
      var two = store.readUpload(2, first, 10, 256);
      assertEquals(one.totalLength(), 10 + one.bytes().length);
      assertEquals(two.totalLength(), 10 + two.bytes().length);
      assertEquals(0, store.activeUploads());
      store.releaseObsolete();
      assertFalse(Files.exists(store.path(first)));
      assertThrows(SnapshotStore.Unavailable.class, () -> store.readUpload(1, first, 0, 10));
    }
  }
}

package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;

class SnapshotTransferStateTest {
  @Test
  void admissionRejectionCancelsDownloadAndAllowsRetry() {
    var effects = new ArrayList<QuorumEffect>();
    var ids = new AtomicLong();
    var identity = ControllerTestSupport.identity(1);
    var transfer =
        new SnapshotTransfer(
            256,
            65536,
            () -> 0,
            request ->
                new Frame(
                    (short) 105,
                    false,
                    identity.clusterId(),
                    1,
                    ids.incrementAndGet(),
                    identity.voterHash(),
                    request),
            () -> new DiskToken(ids.incrementAndGet(), 7, new UUID(0, 1)),
            effects::add);
    var id = new SnapshotId(10, 6, new UUID(0, 10));
    transfer.begin(id, 7);
    var begin = (QuorumEffect.BeginDownload) effects.removeFirst();
    assertTrue(transfer.onCompletion(new DiskDone(begin.token(), new DiskResult.Overloaded())));
    assertFalse(transfer.active());
    assertInstanceOf(QuorumEffect.CancelDownload.class, effects.removeFirst());
    transfer.begin(id, 7);
    var retry = (QuorumEffect.BeginDownload) effects.removeFirst();
    transfer.onCompletion(new DiskDone(retry.token(), new DiskResult.DownloadStarted(id)));
    assertInstanceOf(QuorumEffect.Send.class, effects.removeFirst());
  }

  @Test
  void missingSnapshotCancelsDownloadAndOldResponsesCannotRestartIt() {
    var effects = new ArrayList<QuorumEffect>();
    var ids = new AtomicLong();
    var identity = ControllerTestSupport.identity(1);
    var transfer =
        new SnapshotTransfer(
            256,
            65536,
            () -> 0,
            request ->
                new Frame(
                    (short) 105,
                    false,
                    identity.clusterId(),
                    1,
                    ids.incrementAndGet(),
                    identity.voterHash(),
                    request),
            () -> new DiskToken(ids.incrementAndGet(), 7, new UUID(0, 1)),
            effects::add);
    var id = new SnapshotId(10, 6, new UUID(0, 10));
    transfer.begin(id, 7);
    var begin = (QuorumEffect.BeginDownload) effects.removeFirst();
    transfer.onCompletion(new DiskDone(begin.token(), new DiskResult.DownloadStarted(id)));
    var send = (QuorumEffect.Send) effects.removeFirst();
    var error =
        new Frame(
            (short) 105,
            true,
            identity.clusterId(),
            0,
            send.frame().requestId(),
            identity.voterHash(),
            new Failure(new ReplyMeta(QuorumError.SNAPSHOT_NOT_FOUND, "", 7, 0)));
    assertTrue(transfer.acceptFrame(error));
    assertFalse(transfer.active());
    assertFalse(transfer.acceptFrame(error));
    assertInstanceOf(QuorumEffect.CancelDownload.class, effects.getFirst());
  }

  @Test
  void chunkCompletionPrecedesNextRequestAndCancellationInvalidatesInstall() {
    var effects = new ArrayList<QuorumEffect>();
    var ids = new AtomicLong();
    var identity = ControllerTestSupport.identity(1);
    var transfer =
        new SnapshotTransfer(
            256,
            65536,
            () -> 0,
            request ->
                new Frame(
                    (short) 105,
                    false,
                    identity.clusterId(),
                    1,
                    ids.incrementAndGet(),
                    identity.voterHash(),
                    request),
            () -> new DiskToken(ids.incrementAndGet(), 7, new UUID(0, 1)),
            effects::add);
    var id = new SnapshotId(10, 6, new UUID(0, 10));
    transfer.begin(id, 7);
    var begin = (QuorumEffect.BeginDownload) effects.removeFirst();
    transfer.onCompletion(new DiskDone(begin.token(), new DiskResult.DownloadStarted(id)));
    assertInstanceOf(QuorumEffect.Send.class, effects.removeFirst());
    var reply =
        new FetchSnapshotReply(
            new ReplyMeta(QuorumError.NONE, "", 7, 0), id, 0, 114, new byte[114]);
    transfer.accept(reply);
    transfer.accept(reply);
    assertEquals(1, effects.size());
    var write = (QuorumEffect.WriteSnapshotChunk) effects.removeFirst();
    transfer.onCompletion(new DiskDone(write.token(), new DiskResult.ChunkWritten(id, 114)));
    var finish = (QuorumEffect.FinishDownload) effects.removeFirst();
    transfer.onCompletion(
        new DiskDone(
            finish.token(), new DiskResult.DownloadFinished(id, new MetadataImage(10, List.of()))));
    var install = (QuorumEffect.InstallSnapshot) effects.removeFirst();
    assertTrue(transfer.installAllowed(install.token()));
    transfer.cancel();
    assertFalse(transfer.installAllowed(install.token()));
  }
}

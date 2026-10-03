package vn.huyqt.logbroker.controller.support;

import java.io.IOException;
import java.util.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.StateJournal;
import vn.huyqt.logbroker.controller.snapshot.*;

/** Controllable disk completions; volatile and forced prefixes are intentionally distinct. */
public final class FakeDisk {
  private final ArrayDeque<QuorumEffect.DiskEffect> pending = new ArrayDeque<>();
  private List<QuorumBatch> batches = new ArrayList<>(), forced = new ArrayList<>();
  private long epoch, committed;
  private int vote = -1;
  private boolean failForce;
  private long start, lastEpoch;
  private UUID generation;
  private SnapshotId downloading;
  private byte[] download = new byte[0];
  private final Map<SnapshotId, byte[]> snapshots = new HashMap<>();
  private MetadataImage installedImage = new MetadataImage(0, List.of());

  public FakeDisk() {
    this(new UUID(0, 1));
  }

  public FakeDisk(UUID generation) {
    this.generation = generation;
  }

  public UUID generation() {
    return generation;
  }

  public MetadataImage installedImage() {
    return installedImage;
  }

  public void enqueue(QuorumEffect.DiskEffect work) {
    if (pending.size() >= 256) throw new AssertionError("Fake disk queue unbounded");
    pending.addLast(work);
  }

  public int pending() {
    return pending.size();
  }

  public void failNextForce() {
    failForce = true;
  }

  public long epoch() {
    return epoch;
  }

  public int votedFor() {
    return vote;
  }

  /** Deliberately broken fixtures used only to prove the independent oracle detects vote loss. */
  public void forgetVoteForTest() {
    vote = -1;
  }

  public void overwriteVoteForTest(long epoch, int voter) {
    this.epoch = epoch;
    vote = voter;
  }

  public long committed() {
    return committed;
  }

  public EpochIndex index() {
    return new EpochIndex(start, lastEpoch, batches);
  }

  public long durable() {
    return forced.isEmpty() ? start : forced.getLast().nextOffset();
  }

  public List<QuorumBatch> batches() {
    return List.copyOf(batches);
  }

  public QuorumEvent completeNext() {
    var work = pending.removeFirst();
    try {
      return new DiskDone(work.token(), execute(work));
    } catch (Exception e) {
      return new DiskFailed(work.token(), e.toString());
    }
  }

  private DiskResult execute(QuorumEffect.DiskEffect work) throws IOException {
    if (!(work instanceof QuorumEffect.PersistVote)
        && !work.token().generation().equals(generation)) return new DiskResult.Discarded();
    return switch (work) {
      case QuorumEffect.PersistVote p -> {
        forceBoundary();
        if (p.epoch() < epoch || p.epoch() == epoch && vote != -1 && vote != p.voter())
          throw new IOException("Double vote");
        epoch = p.epoch();
        vote = p.voter();
        yield new DiskResult.VoteSaved(epoch, vote);
      }
      case QuorumEffect.Append p -> {
        var batch = new QuorumBatch(index().end(), p.entries());
        batches.add(batch);
        yield new DiskResult.Appended(batch, index(), durable());
      }
      case QuorumEffect.AppendReplica p -> {
        if (p.batch().baseOffset() != index().end()) throw new IOException("Append gap");
        batches.add(p.batch());
        yield new DiskResult.Appended(p.batch(), index(), durable());
      }
      case QuorumEffect.Flush ignored -> {
        forceBoundary();
        forced = new ArrayList<>(batches);
        yield new DiskResult.Flushed(durable());
      }
      case QuorumEffect.Truncate p -> {
        if (p.end() < committed) throw new IOException("Truncate committed data");
        index().positionAt(p.end());
        batches = new ArrayList<>(batches.stream().filter(b -> b.nextOffset() <= p.end()).toList());
        forced = new ArrayList<>(batches);
        yield new DiskResult.Truncated(index());
      }
      case QuorumEffect.Checkpoint p -> {
        forceBoundary();
        if (p.end() > durable() || p.end() < committed)
          throw new IOException("Invalid commit checkpoint");
        committed = p.end();
        yield new DiskResult.Checkpointed(committed);
      }
      case QuorumEffect.ReadObserver p -> p.offset() < start
          ? new DiskResult.SnapshotRejected(vn.huyqt.logbroker.controller.protocol.QuorumError.SNAPSHOT_NOT_FOUND)
          : new DiskResult.Read(
          vn.huyqt.logbroker.controller.metadata.ObserverReadService.committedPrefix(
              ((DiskResult.Read)execute(new QuorumEffect.ReadLog(p.token(),p.offset(),p.budget()))).batches(),p.upperBound()));
      case QuorumEffect.ReadObserverSnapshot p -> execute(new QuorumEffect.ReadSnapshotChunk(
          p.token(),p.brokerId(),p.id(),p.position(),p.maxBytes()));
      case QuorumEffect.MaintainUploads ignored -> new DiskResult.Discarded();
      case QuorumEffect.ReadLog p -> {
        var result = new ArrayList<QuorumBatch>();
        long bytes = 0;
        for (var batch : batches) {
          if (batch.nextOffset() <= p.offset()) continue;
          long size = 16;
          for (var e : batch.entries()) size += 4 + QuorumEntryCodec.encode(e).length;
          if (!result.isEmpty() && bytes + size > p.budget()) break;
          result.add(batch);
          bytes += size;
          if (bytes >= p.budget()) break;
        }
        yield new DiskResult.Read(result);
      }
      case QuorumEffect.InstallSnapshot install -> {
        forceBoundary();
        if (install.id().endOffset() < committed) throw new IOException("Install crosses commit");
        start = install.id().endOffset();
        lastEpoch = install.id().lastEpoch();
        batches.clear();
        forced.clear();
        committed = start;
        installedImage = install.image();
        generation = UUID.randomUUID();
        yield new DiskResult.Installed(generation, install.id(), index(), install.image());
      }
      case QuorumEffect.CreateSnapshot create -> {
        forceBoundary();
        var id =
            new SnapshotId(create.image().appliedOffset(), create.lastEpoch(), UUID.randomUUID());
        snapshots.put(id, encode(id, create.image()));
        yield new DiskResult.SnapshotCreated(id);
      }
      case QuorumEffect.RetainSnapshotPrefix retain ->
          throw new IOException("Fake retention not connected");
      case QuorumEffect.BeginDownload begin -> {
        downloading = begin.id();
        download = new byte[0];
        yield new DiskResult.DownloadStarted(downloading);
      }
      case QuorumEffect.WriteSnapshotChunk chunk -> {
        if (!chunk.id().equals(downloading) || chunk.position() != download.length)
          throw new IOException("Invalid fake chunk");
        var next = Arrays.copyOf(download, download.length + chunk.bytes().length);
        System.arraycopy(chunk.bytes(), 0, next, download.length, chunk.bytes().length);
        download = next;
        yield new DiskResult.ChunkWritten(downloading, download.length);
      }
      case QuorumEffect.FinishDownload finish -> {
        forceBoundary();
        if (!finish.id().equals(downloading)
            || finish.totalLength() != download.length
            || download.length < 114
            || java.nio.ByteBuffer.wrap(download).getInt(download.length - 4)
                != StateJournal.crc(download, 0, download.length - 4))
          throw new IOException("Invalid fake snapshot");
        snapshots.put(downloading, download);
        var image =
            java.nio.ByteBuffer.wrap(download).getShort(4) == 1
                ? MetadataImageCodec.decode(Arrays.copyOfRange(download, 98, download.length - 4))
                : MetadataImageCodec.decodeV2(Arrays.copyOfRange(download, 98, download.length - 4), MetadataLimits.defaults());
        yield new DiskResult.DownloadFinished(downloading, image);
      }
      case QuorumEffect.CancelDownload ignored -> {
        downloading = null;
        download = new byte[0];
        yield new DiskResult.Discarded();
      }
      case QuorumEffect.ReadSnapshotChunk read -> {
        var bytes = snapshots.get(read.id());
        if (bytes == null)
          yield new DiskResult.SnapshotRejected(
              vn.huyqt.logbroker.controller.protocol.QuorumError.SNAPSHOT_NOT_FOUND);
        int end = (int) Math.min(bytes.length, read.position() + read.maxBytes());
        yield new DiskResult.SnapshotChunk(
            read.id(),
            read.position(),
            bytes.length,
            Arrays.copyOfRange(bytes, (int) read.position(), end));
      }
    };
  }

  private void forceBoundary() throws IOException {
    if (failForce) {
      failForce = false;
      throw new IOException("Injected force failure");
    }
  }

  public int pendingSnapshots() {
    return (int) pending.stream().filter(QuorumEffect.CreateSnapshot.class::isInstance).count();
  }

  public long downloadedBytes() {
    return download.length;
  }

  public boolean hasPublishedGenerationFor(SnapshotId id) {
    return start == id.endOffset() && installedImage.appliedOffset() == start;
  }

  public SnapshotId compact(MetadataImage image) throws IOException {
    var id =
        new SnapshotId(
            image.appliedOffset(),
            index().positionAt(image.appliedOffset()).lastEpoch(),
            UUID.randomUUID());
    snapshots.put(id, encode(id, image));
    start = id.endOffset();
    lastEpoch = id.lastEpoch();
    batches = new ArrayList<>(batches.stream().filter(b -> b.baseOffset() >= start).toList());
    forced = new ArrayList<>(forced.stream().filter(b -> b.baseOffset() >= start).toList());
    installedImage = image;
    return id;
  }

  private byte[] encode(SnapshotId id, MetadataImage image) throws IOException {
    byte[] payload = image.metadataVersion() == 1 ? MetadataImageCodec.encode(image)
        : MetadataImageCodec.encodeV2(image, MetadataLimits.defaults());
    byte[] bytes = new byte[102 + payload.length];
    var identity = ControllerTestSupport.identity(0);
    var out = java.nio.ByteBuffer.wrap(bytes);
    out.putInt(0x51534e31)
        .putShort((short) (image.metadataVersion() == 1 ? 1 : 2))
        .putLong(bytes.length)
        .putLong(identity.clusterId().getMostSignificantBits())
        .putLong(identity.clusterId().getLeastSignificantBits())
        .put(identity.voterHash());
    id.writeTo(out);
    out.putInt(payload.length).put(payload).putInt(StateJournal.crc(bytes, 0, bytes.length - 4));
    return bytes;
  }

  public void powerLoss() {
    batches = new ArrayList<>(forced);
    pending.clear();
  }

  public void processCrash() {
    pending.clear();
  }

  public void recover() {
    forced = new ArrayList<>(batches);
  }
}

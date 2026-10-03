package vn.huyqt.logbroker.controller.runtime;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.metadata.MetadataStateMachine;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotStore;

/**
 * Executes disk work off-loop, but applies committed metadata on the state-owning loop.
 *
 * <p>Translates {@link QuorumEffect}s from the consensus state machine into actions: disk effects
 * run on the {@link OrderedDiskExecutor} against the {@link GenerationStore}, {@link
 * QuorumStateStore} and {@link SnapshotStore}; {@code Apply} and {@code Restore} update the {@link
 * MetadataStateMachine} synchronously and report {@code Applied}; everything else goes to the
 * external consumer. {@link #run} must be called on the controller loop; disk bodies run on the
 * disk worker.
 */
public final class EffectRunner {
  private final QuorumStateStore state;
  private final GenerationStore generation;
  private final MetadataStateMachine metadata;
  private final OrderedDiskExecutor disk;
  private final Consumer<QuorumEvent> events;
  private final Consumer<QuorumEffect> external;
  private SnapshotStore snapshots;
  private java.util.function.Predicate<DiskToken> installFence = ignored -> false;

  /**
   * Sets the predicate that decides, on the disk worker, whether an {@code InstallSnapshot} may
   * still publish. It is checked before install starts and again inside {@link
   * GenerationStore#install}. Until set, every install is discarded. Call before running effects.
   */
  public void installFence(java.util.function.Predicate<DiskToken> fence) {
    installFence = fence;
  }

  public EffectRunner(
      QuorumStateStore state,
      GenerationStore generation,
      MetadataStateMachine metadata,
      OrderedDiskExecutor disk,
      Consumer<QuorumEvent> events,
      Consumer<QuorumEffect> external) {
    this.state = state;
    this.generation = generation;
    this.metadata = metadata;
    this.disk = disk;
    this.events = events;
    this.external = external;
  }

  /** Connects the snapshot store used by snapshot effects. Call before running effects. */
  public void snapshots(SnapshotStore snapshots) {
    this.snapshots = snapshots;
  }

  /**
   * Runs effects in order. A disk effect that the executor refuses is answered at once with an
   * {@code Overloaded} completion instead of being dropped. A metadata apply or restore failure is
   * reported to the external consumer as {@code Fail}.
   */
  public void run(List<QuorumEffect> effects) {
    for (var effect : effects) {
      if (effect instanceof QuorumEffect.DiskEffect work) {
        // Lifecycle and feature initialization share the reserved consensus completion slots.
        // Topic/admin appends may be refused under pressure; snapshots run at low priority.
        boolean ordinary =
            work instanceof QuorumEffect.MaintainUploads
                || work instanceof QuorumEffect.ReadObserver || work instanceof QuorumEffect.ReadObserverSnapshot
                || work instanceof QuorumEffect.Append append
                && !(append.entries().getFirst()
                    instanceof vn.huyqt.logbroker.controller.log.QuorumEntry.LeaderChange)
                && !(append.entries().getFirst() instanceof vn.huyqt.logbroker.controller.log.QuorumEntry.BrokerState)
                && !(append.entries().getFirst() instanceof vn.huyqt.logbroker.controller.log.QuorumEntry.FeatureLevel);
        boolean accepted =
            work instanceof QuorumEffect.CreateSnapshot
                ? disk.submitLowPriority(work.token(), () -> execute(work))
                : ordinary
                    ? disk.submitOrdinary(work.token(), () -> execute(work))
                    : disk.submit(work.token(), () -> execute(work));
        if (!accepted) events.accept(new DiskDone(work.token(), new DiskResult.Overloaded()));
      } else if (effect instanceof QuorumEffect.Apply apply) {
        // Apply carries its generation; one from a replaced generation is skipped. See
        // docs/controller-storage-v1.md.
        if (apply.generation() != null && !apply.generation().equals(generation.generation()))
          continue;
        try {
          for (var batch : apply.batches()) metadata.apply(batch);
          events.accept(
              new Applied(metadata.image().appliedOffset(), metadata.image(), apply.generation()));
        } catch (IOException e) {
          external.accept(new QuorumEffect.Fail(e.toString()));
        }
      } else if (effect instanceof QuorumEffect.Restore restore) {
        try {
          metadata.restore(restore.image());
          events.accept(
              new Applied(
                  metadata.image().appliedOffset(), metadata.image(), generation.generation()));
        } catch (IOException e) {
          external.accept(new QuorumEffect.Fail(e.toString()));
        }
      } else if (effect instanceof QuorumEffect.Enqueue enqueue) events.accept(enqueue.event());
      else external.accept(effect);
    }
  }

  // Runs on the disk worker. Work stamped with an old generation is discarded; votes are global
  // hard state and are persisted regardless of generation.
  private DiskResult execute(QuorumEffect.DiskEffect effect) throws IOException {
    if (!(effect instanceof QuorumEffect.PersistVote) && !(effect instanceof QuorumEffect.MaintainUploads)
        && !effect.token().generation().equals(generation.generation()))
      return new DiskResult.Discarded();
    return switch (effect) {
      case QuorumEffect.PersistVote vote -> {
        state.persistVote(vote.epoch(), vote.voter());
        yield new DiskResult.VoteSaved(state.epoch(), state.votedFor());
      }
      case QuorumEffect.Append append -> {
        var batch = generation.log().append(append.epoch(), append.entries());
        yield new DiskResult.Appended(
            batch, generation.log().epochs(), generation.log().durableEnd());
      }
      case QuorumEffect.AppendReplica append -> {
        generation.log().appendReplica(append.batch());
        yield new DiskResult.Appended(
            append.batch(), generation.log().epochs(), generation.log().durableEnd());
      }
      case QuorumEffect.Flush ignored -> new DiskResult.Flushed(generation.log().flush());
      case QuorumEffect.Truncate truncate -> {
        generation.truncate(truncate.end());
        yield new DiskResult.Truncated(generation.log().epochs());
      }
      case QuorumEffect.Checkpoint checkpoint -> {
        generation.checkpointCommit(checkpoint.end());
        yield new DiskResult.Checkpointed(checkpoint.end());
      }
      case QuorumEffect.ReadLog read ->
          new DiskResult.Read(generation.log().read(read.offset(), read.budget()));
      case QuorumEffect.ReadObserver read -> read.offset() < generation.log().start()
          ? new DiskResult.SnapshotRejected(vn.huyqt.logbroker.controller.protocol.QuorumError.SNAPSHOT_NOT_FOUND)
          : new DiskResult.Read(vn.huyqt.logbroker.controller.metadata.ObserverReadService.committedPrefix(
              generation.log().read(read.offset(),read.budget()),read.upperBound()));
      case QuorumEffect.ReadObserverSnapshot read -> {
        try {
          var chunk = snapshots.readObserverUpload(read.brokerId(),read.id(),read.position(),read.maxBytes());
          yield new DiskResult.SnapshotChunk(chunk.id(),chunk.position(),chunk.totalLength(),chunk.bytes());
        } catch (SnapshotStore.Unavailable unavailable) {
          yield new DiskResult.SnapshotRejected(unavailable.error());
        }
      }
      case QuorumEffect.MaintainUploads maintenance -> {
        snapshots.expireUploads(System.nanoTime(),maintenance.closeObservers()); yield new DiskResult.Discarded();
      }
      case QuorumEffect.InstallSnapshot install -> {
        if (!installFence.test(install.token())) yield new DiskResult.Discarded();
        try {
          generation.install(
              install.id(), install.image(), () -> installFence.test(install.token()));
        } catch (GenerationStore.InstallCancelled cancelled) {
          yield new DiskResult.Discarded();
        }
        // Install published a new SNAPSHOT_SET through its own SnapshotStore; resync this one.
        snapshots.refreshRetained();
        generation.releaseObsoleteGenerations();
        yield new DiskResult.Installed(
            generation.generation(), install.id(), generation.epochIndex(), install.image());
      }
      case QuorumEffect.BeginDownload begin -> {
        snapshots.beginDownload(begin.id());
        yield new DiskResult.DownloadStarted(begin.id());
      }
      case QuorumEffect.WriteSnapshotChunk chunk -> {
        snapshots.writeChunk(chunk.id(), chunk.position(), chunk.bytes());
        yield new DiskResult.ChunkWritten(chunk.id(), snapshots.downloadedBytes());
      }
      case QuorumEffect.FinishDownload finish ->
          new DiskResult.DownloadFinished(
              finish.id(), snapshots.finishDownload(finish.id(), finish.totalLength()));
      case QuorumEffect.CancelDownload ignored -> {
        snapshots.cancelDownload();
        yield new DiskResult.Discarded();
      }
      case QuorumEffect.ReadSnapshotChunk read -> {
        try {
          var chunk =
              snapshots.readUpload(read.peer(), read.id(), read.position(), read.maxBytes());
          yield new DiskResult.SnapshotChunk(
              chunk.id(), chunk.position(), chunk.totalLength(), chunk.bytes());
        } catch (SnapshotStore.Unavailable unavailable) {
          yield new DiskResult.SnapshotRejected(unavailable.error());
        }
      }
      case QuorumEffect.CreateSnapshot create -> {
        if (snapshots == null) throw new IOException("Snapshot store not connected");
        yield new DiskResult.SnapshotCreated(snapshots.create(create.image(), create.lastEpoch()));
      }
      case QuorumEffect.RetainSnapshotPrefix retain -> {
        generation.retainPrefix(retain.olderEnd());
        snapshots.releaseObsolete();
        yield new DiskResult.PrefixRetained(generation.epochIndex(), generation.baseSnapshot());
      }
    };
  }
}

package vn.huyqt.logbroker.controller.snapshot;

import java.util.*;
import java.util.function.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/**
 * One loop-owned download. RPC correlation and disk completion gate every next step.
 *
 * <p>Drives a follower's snapshot fetch from the leader as a sequence of effects: begin the
 * download file, request one chunk at a time, write each accepted chunk, finish the file, and
 * install it. Only one {@code FetchSnapshot} request is outstanding at a time and the next one is
 * sent only after the previous chunk's write completes. Any invalid reply cancels the whole
 * transfer rather than retrying from the middle.
 *
 * <p>Owned by the consensus state machine and confined to the controller loop, except {@link
 * #installAllowed}, which the disk worker calls through the install fence.
 */
public final class SnapshotTransfer {
  private final int chunkBytes, maxBytes;
  private final IntSupplier leader;
  private final Function<Request, Frame> request;
  private final Supplier<DiskToken> tokens;
  private final Consumer<QuorumEffect> emit;
  private final Map<DiskToken, Consumer<DiskResult>> pending = new HashMap<>();
  private SnapshotId id;
  private long epoch, position, total = -1, now, flightDeadline;
  private Frame flight;

  // Stores a SHA-256 digest of the last accepted chunk rather than its bytes; used only to
  // recognise an exact repeat of that chunk.
  private record AcceptedChunk(SnapshotId id, long position, long total, int length, byte[] hash) {
    static AcceptedChunk of(FetchSnapshotReply reply) {
      byte[] bytes = reply.chunk();
      try {
        return new AcceptedChunk(
            reply.id(),
            reply.position(),
            reply.totalLength(),
            bytes.length,
            java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
      } catch (java.security.NoSuchAlgorithmException impossible) {
        throw new IllegalStateException(impossible);
      }
    }

    boolean matches(AcceptedChunk other) {
      return id.equals(other.id)
          && position == other.position
          && total == other.total
          && length == other.length
          && Arrays.equals(hash, other.hash);
    }
  }

  private int source;
  private boolean writing;
  private AcceptedChunk lastChunk;
  // Volatile because the disk worker reads it through installAllowed while the loop clears it.
  private volatile DiskToken installToken;
  private long rpcNanos = 1_000_000_000L;

  /**
   * Creates a transfer with a one-second chunk RPC timeout.
   *
   * @param chunkBytes largest chunk requested and accepted
   * @param maxBytes configured snapshot size limit
   * @param leader current leader ID; sampled once per {@link #begin}
   * @param request wraps a request body into a frame with a fresh request ID
   * @param tokens allocates disk tokens for the effects this transfer emits
   * @param emit receives {@code Send} and download disk effects
   */
  public SnapshotTransfer(
      int chunkBytes,
      int maxBytes,
      IntSupplier leader,
      Function<Request, Frame> request,
      Supplier<DiskToken> tokens,
      Consumer<QuorumEffect> emit) {
    this.chunkBytes = chunkBytes;
    this.maxBytes = maxBytes;
    this.leader = leader;
    this.request = request;
    this.tokens = tokens;
    this.emit = emit;
  }

  /**
   * Creates a transfer with an explicit chunk RPC timeout.
   *
   * @param rpcNanos time after which an unanswered chunk request is re-sent; must be positive
   */
  public SnapshotTransfer(
      int chunkBytes,
      int maxBytes,
      long rpcNanos,
      IntSupplier leader,
      Function<Request, Frame> request,
      Supplier<DiskToken> tokens,
      Consumer<QuorumEffect> emit) {
    this(chunkBytes, maxBytes, leader, request, tokens, emit);
    if (rpcNanos <= 0) throw new IllegalArgumentException("Invalid snapshot RPC timeout");
    this.rpcNanos = rpcNanos;
  }

  /**
   * Cancels any current transfer and starts downloading {@code id} from the current leader. The
   * first chunk request is sent only after the download file has been created.
   *
   * @param epoch epoch sent in every chunk request; replies must echo it
   */
  public void begin(SnapshotId id, long epoch) {
    cancel();
    this.id = id;
    this.epoch = epoch;
    source = leader.getAsInt();
    position = 0;
    total = -1;
    lastChunk = null;
    var token = tokens.get();
    pending.put(token, result -> send());
    emit.accept(new QuorumEffect.BeginDownload(token, id));
  }

  // Every send, including a timeout retry, uses a new request ID, so a late reply to an earlier
  // attempt is no longer correlated.
  private void send() {
    if (id == null) return;
    flight = request.apply(new FetchSnapshot(epoch, id, position, chunkBytes));
    flightDeadline = now + rpcNanos;
    emit.accept(new QuorumEffect.Send(source, flight));
  }

  /**
   * Returns whether {@code frame} answers the outstanding chunk request: same request ID, sent by
   * the leader chosen at {@link #begin}, for operation {@code FetchSnapshot}.
   */
  public boolean correlated(Frame frame) {
    return flight != null
        && frame.requestId() == flight.requestId()
        && frame.senderId() == source
        && frame.operation() == 105;
  }

  /**
   * Consumes a correlated reply frame. A reply that is not a {@code FetchSnapshotReply}, such as an
   * error reply, cancels the transfer.
   *
   * @return whether the frame was correlated and consumed
   */
  public boolean acceptFrame(Frame frame) {
    if (!correlated(frame)) return false;
    if (frame.message() instanceof FetchSnapshotReply reply) accept(reply);
    else cancel();
    return true;
  }

  /**
   * Accepts the chunk at the expected position and emits a disk write for it. An exact repeat of
   * the last accepted chunk is ignored. Any other mismatch (snapshot ID, epoch, error, position,
   * total length, chunk size, or a reply while a write is pending) cancels the transfer.
   */
  public void accept(FetchSnapshotReply reply) {
    if (id == null) return;
    var received = AcceptedChunk.of(reply);
    if (lastChunk != null && lastChunk.matches(received)) return;
    if (writing
        || !id.equals(reply.id())
        || reply.meta().epoch() != epoch
        || reply.meta().error() != QuorumError.NONE
        || reply.position() != position
        // Same file-size bounds as SnapshotStore; see docs/controller-storage-v1.md.
        || reply.totalLength() < 114
        || reply.totalLength() > Math.min(maxBytes, 512 + 128 * 281)
        || total != -1 && total != reply.totalLength()
        || reply.chunk().length == 0
        || reply.chunk().length > chunkBytes
        || position > reply.totalLength() - reply.chunk().length) {
      cancel();
      return;
    }
    total = reply.totalLength();
    lastChunk = received;
    flight = null;
    writing = true;
    var token = tokens.get();
    pending.put(
        token,
        result -> {
          writing = false;
          position = ((DiskResult.ChunkWritten) result).end();
          if (position == total) finish();
          else send();
        });
    emit.accept(new QuorumEffect.WriteSnapshotChunk(token, id, position, reply.chunk()));
  }

  // The install token exists only after the finished file has been validated; installAllowed
  // accepts no other token.
  private void finish() {
    var token = tokens.get();
    pending.put(
        token,
        result -> {
          var done = (DiskResult.DownloadFinished) result;
          installToken = tokens.get();
          pending.put(
              installToken,
              ignored -> {
                id = null;
                installToken = null;
              });
          emit.accept(new QuorumEffect.InstallSnapshot(installToken, done.id(), done.image()));
        });
    emit.accept(new QuorumEffect.FinishDownload(token, id, total));
  }

  /**
   * Advances the transfer on completion of one of its disk effects. {@code Overloaded} cancels the
   * transfer; {@code Discarded} is consumed without advancing.
   *
   * @return whether {@code done} belonged to this transfer
   */
  public boolean onCompletion(DiskDone done) {
    var callback = pending.remove(done.token());
    if (callback == null) return false;
    if (done.result() instanceof DiskResult.Overloaded) {
      cancel();
      return true;
    }
    if (!(done.result() instanceof DiskResult.Discarded)) callback.accept(done.result());
    return true;
  }

  /** Records loop time and re-sends the outstanding chunk request once its deadline passes. */
  public void onTick(long now) {
    this.now = now;
    if (flight != null && now >= flightDeadline) send();
  }

  /**
   * Returns whether {@code token} is the pending install of this transfer. Safe to call from the
   * disk worker. Becomes false after {@link #cancel}, which an {@code Overloaded} completion also
   * triggers, or after the install completes with any result other than {@code Discarded}.
   */
  public boolean installAllowed(DiskToken token) {
    return token.equals(installToken);
  }

  public boolean active() {
    return id != null;
  }

  /**
   * Abandons the transfer and forgets all pending disk callbacks, so their completions are no
   * longer recognised. Emits {@code CancelDownload} only if a transfer was active.
   */
  public void cancel() {
    boolean had = id != null;
    id = null;
    flight = null;
    installToken = null;
    writing = false;
    lastChunk = null;
    pending.clear();
    if (had) emit.accept(new QuorumEffect.CancelDownload(tokens.get()));
  }
}

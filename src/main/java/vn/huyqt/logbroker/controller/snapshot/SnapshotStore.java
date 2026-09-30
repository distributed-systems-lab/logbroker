package vn.huyqt.logbroker.controller.snapshot;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardOpenOption.READ;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.protocol.QuorumError;

/**
 * Immutable snapshots are visible only through a forced journal publication.
 *
 * <p>Owns {@code snapshots/} under a controller root: locally created snapshots, the single
 * in-progress download from a leader, and read pins for uploads to followers. A snapshot file is
 * forced and its directory entry synced before a {@code SNAPSHOT_SET} frame in the {@link
 * StateJournal} names it; the retained set is rebuilt from the last such frame on open. Size
 * bounds, retention and download rules are specified in {@code docs/controller-storage-v1.md}.
 *
 * <p>Download, upload and pin state is guarded by this instance's monitor. {@link #create},
 * {@link #publishInstalled} and {@link #refreshRetained} are not synchronized and must be
 * serialized by the caller. {@link #retained()} is volatile and may be read from any thread.
 */
public final class SnapshotStore implements AutoCloseable {
  private final Path directory;
  private final ClusterIdentity identity;
  private final DurableFiles files;
  private final QuorumStateStore state;
  private final int maxBytes;
  private volatile List<SnapshotId> retained = List.of();
  private final Map<SnapshotId, Integer> pins = new HashMap<>();
  private SnapshotId downloading;
  private FileChannel download;
  private long downloaded;

  /**
   * An upload request that cannot be served; {@link #error()} is the protocol error to return to
   * the requesting peer instead of failing the node.
   */
  public static final class Unavailable extends IOException {
    private final QuorumError error;

    public Unavailable(QuorumError error) {
      super(error.name());
      this.error = error;
    }

    public QuorumError error() {
      return error;
    }
  }

  /** One slice of a pinned snapshot file served to a peer. */
  public record UploadChunk(SnapshotId id, long position, long totalLength, byte[] bytes) {}

  // One upload session per peer; the pin keeps the file from being released while it is read.
  private final class Upload {
    final SnapshotId id;
    final Pin pin;
    long accessed;

    Upload(SnapshotId id, Pin pin, long accessed) {
      this.id = id;
      this.pin = pin;
      this.accessed = accessed;
    }
  }

  private final Map<Integer, Upload> uploads = new HashMap<>();

  /**
   * Opens the snapshot directory of {@code root} and recovers the retained set from the last
   * {@code SNAPSHOT_SET} journal frame. Every retained snapshot is fully decoded and validated.
   *
   * @param maxBytes configured upper bound on a snapshot file, in bytes
   * @throws IllegalArgumentException if {@code maxBytes} cannot hold a minimal snapshot or exceeds
   *     64 MiB
   * @throws IOException if the directory is missing, a {@code SNAPSHOT_SET} frame is malformed, or
   *     a retained snapshot is missing or invalid
   */
  public SnapshotStore(
      Path root, ClusterIdentity identity, DurableFiles files, QuorumStateStore state, int maxBytes)
      throws IOException {
    directory = root.resolve("snapshots");
    this.identity = identity;
    this.files = files;
    this.state = state;
    this.maxBytes = maxBytes;
    // 114 bytes is the fixed 102-byte envelope plus the 12-byte minimum payload decode accepts.
    if (maxBytes < 114 || maxBytes > 64 * 1024 * 1024)
      throw new IllegalArgumentException("Invalid snapshot limit");
    if (!Files.isDirectory(directory))
      throw new IOException("Published snapshot directory missing");
    // Each SNAPSHOT_SET replaces the previous one, so the last frame is authoritative.
    for (var frame : state.journal().frames())
      if (frame.type() == StateJournal.SNAPSHOT_SET) {
        var in = ByteBuffer.wrap(frame.payload());
        try {
          int count = in.getInt();
          if (count < 0 || count > 2 || in.remaining() != count * 32)
            throw new IOException("Invalid snapshot set");
          var ids = new ArrayList<SnapshotId>();
          for (int i = 0; i < count; i++) ids.add(SnapshotId.readFrom(in));
          if (ids.stream().map(SnapshotId::contentId).distinct().count() != count)
            throw new IOException("Duplicate snapshot IDs");
          retained = List.copyOf(ids);
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
          throw new IOException("Invalid snapshot journal", e);
        }
      }
    for (var id : retained) load(id);
  }

  /**
   * Writes {@code image} as a new immutable snapshot and publishes it as the newest retained one.
   *
   * <p>The file is written and forced under a {@code .partial} name, atomically renamed, and the
   * directory synced before the {@code SNAPSHOT_SET} frame is appended, so the journal never names
   * a file that is not durable. The previous newest snapshot stays retained as a fallback.
   *
   * @param lastEpoch epoch of the last log entry before {@code image.appliedOffset()}
   * @return the published identity
   * @throws IOException if the image boundary is below the newest retained snapshot, the encoding
   *     exceeds the configured limit, or any write, force or journal append fails
   */
  public SnapshotId create(MetadataImage image, long lastEpoch) throws IOException {
    if (!retained.isEmpty() && image.appliedOffset() < retained.getFirst().endOffset())
      throw new IOException("Snapshot boundary regression");
    var id = new SnapshotId(image.appliedOffset(), lastEpoch, UUID.randomUUID());
    byte[] bytes = encode(id, image);
    Path temporary = directory.resolve(id.contentId() + ".partial");
    files.writeNew(temporary, bytes);
    Files.move(temporary, path(id), StandardCopyOption.ATOMIC_MOVE);
    files.syncDirectory(directory);
    publish(id);
    return id;
  }

  // Keep the previous image until the newer one is journaled, so recovery has a fallback.
  private void publish(SnapshotId id) throws IOException {
    var next = new ArrayList<SnapshotId>();
    next.add(id);
    for (var existing : retained) if (!existing.equals(id) && next.size() < 2) next.add(existing);
    ByteBuffer payload = ByteBuffer.allocate(4 + next.size() * 32).putInt(next.size());
    for (var snapshot : next) snapshot.writeTo(payload);
    state.journal().append(StateJournal.SNAPSHOT_SET, payload.array());
    retained = List.copyOf(next);
  }

  /**
   * Makes an already stored snapshot the only retained one.
   *
   * <p>Used after a snapshot install, and at startup when the retained set lags the active
   * generation's base snapshot. The file is validated before the {@code SNAPSHOT_SET} frame is
   * appended.
   *
   * @throws IOException if the snapshot file is missing or invalid, or the journal append fails
   */
  public void publishInstalled(SnapshotId id) throws IOException {
    load(id);
    var out = ByteBuffer.allocate(36).putInt(1);
    id.writeTo(out);
    state.journal().append(StateJournal.SNAPSHOT_SET, out.array());
    retained = List.of(id);
  }

  /**
   * Starts a download of {@code id} into a fresh {@code .download.partial} file, cancelling any
   * download already in progress. At most one download exists per store.
   */
  public synchronized void beginDownload(SnapshotId id) throws IOException {
    cancelDownload();
    Path path = partial(id);
    Files.deleteIfExists(path);
    download = FileChannel.open(path, CREATE_NEW, READ, WRITE);
    downloading = id;
    downloaded = 0;
  }

  private Path partial(SnapshotId id) {
    return directory.resolve(id.contentId() + ".download.partial");
  }

  /**
   * Writes one chunk of the current download. Chunks must arrive contiguously; a chunk that lies
   * entirely within the already written prefix is accepted only if its bytes match exactly, and is
   * not written again. Chunk data is not forced here; {@link #finishDownload} forces the file.
   *
   * @throws IOException if {@code id} is not the current download, the chunk is empty, larger than
   *     256 KiB, leaves a gap, partially overlaps written bytes, differs from a written duplicate,
   *     or would exceed the snapshot size bound
   */
  public synchronized void writeChunk(SnapshotId id, long position, byte[] bytes)
      throws IOException {
    if (!id.equals(downloading)
        || download == null
        || position < 0
        || bytes.length == 0
        || bytes.length > 256 * 1024
        || position > downloaded
        || position > Math.min(maxBytes, 512 + 128 * 281) - bytes.length)
      throw new IOException("Invalid snapshot chunk");
    if (position < downloaded) {
      if (position + bytes.length > downloaded) throw new IOException("Overlapping snapshot chunk");
      var existing = ByteBuffer.allocate(bytes.length);
      long offset = position;
      while (existing.hasRemaining()) {
        int count = download.read(existing, offset);
        if (count <= 0) throw new IOException("Snapshot duplicate read stalled");
        offset += count;
      }
      if (!Arrays.equals(bytes, existing.array()))
        throw new IOException("Conflicting snapshot duplicate");
      return;
    }
    var buffer = ByteBuffer.wrap(bytes);
    long offset = position;
    while (buffer.hasRemaining()) {
      int count = download.write(buffer, offset);
      if (count <= 0) throw new IOException("Snapshot write stalled");
      offset += count;
    }
    downloaded += bytes.length;
  }

  /**
   * Validates and stores the completed download as an immutable snapshot file.
   *
   * <p>The partial file is forced and fully decoded, which checks cluster, voter set, identity and
   * checksum, before it is renamed into place and the directory synced. If a file with the same
   * content ID already exists, its bytes must be identical. This does not publish the snapshot in
   * the journal; installation does that.
   *
   * @return the decoded image
   * @throws IOException if {@code totalLength} does not match the bytes written or the content is
   *     invalid; the partial file is left for {@link #cancelDownload} to remove
   */
  public synchronized MetadataImage finishDownload(SnapshotId id, long totalLength)
      throws IOException {
    if (!id.equals(downloading)
        || download == null
        || totalLength != downloaded
        || totalLength < 114
        || totalLength > Math.min(maxBytes, 512 + 128 * 281))
      throw new IOException("Snapshot download length mismatch");
    Path temporary = partial(id);
    files.forceFile(temporary);
    var image = decode(id, Files.readAllBytes(temporary));
    download.close();
    download = null;
    if (Files.exists(path(id))) {
      if (!Arrays.equals(Files.readAllBytes(path(id)), Files.readAllBytes(temporary)))
        throw new IOException("Snapshot content ID reused with different bytes");
      Files.delete(temporary);
    } else Files.move(temporary, path(id), StandardCopyOption.ATOMIC_MOVE);
    files.syncDirectory(directory);
    downloading = null;
    downloaded = 0;
    return image;
  }

  public synchronized long downloadedBytes() {
    return downloaded;
  }

  /** Abandons the current download, if any, and deletes its partial file. Idempotent. */
  public synchronized void cancelDownload() throws IOException {
    if (download != null) {
      download.close();
      download = null;
    }
    if (downloading != null) Files.deleteIfExists(partial(downloading));
    downloading = null;
    downloaded = 0;
  }

  /**
   * Reads one chunk of a snapshot for {@code peer}, keeping the file pinned across requests.
   *
   * <p>Each peer has at most one upload session; asking for a different snapshot replaces it. At
   * most two sessions exist at once, sessions idle for 30 seconds are closed, and a session ends
   * after its final chunk is read. Only retained snapshots and the active generation's base
   * snapshot can be served.
   *
   * @param maxBytes chunk size limit requested by the peer, at most 256 KiB
   * @throws Unavailable with {@code SNAPSHOT_NOT_FOUND}, {@code OVERLOADED} or {@code
   *     INVALID_REQUEST} when the request cannot be served
   * @throws IOException if the snapshot file cannot be read or fails validation
   */
  public synchronized UploadChunk readUpload(int peer, SnapshotId id, long position, int maxBytes)
      throws IOException {
    long now = System.nanoTime();
    for (int key : List.copyOf(uploads.keySet()))
      if (now - uploads.get(key).accessed >= 30_000_000_000L) {
        uploads.remove(key).pin.close();
      }
    var upload = uploads.get(peer);
    if (upload != null && !upload.id.equals(id)) {
      uploads.remove(peer).pin.close();
      upload = null;
    }
    if (upload == null) {
      if (!retained.contains(id) && !id.equals(GenerationStore.snapshotReference(state)))
        throw new Unavailable(QuorumError.SNAPSHOT_NOT_FOUND);
      if (uploads.size() >= 2) throw new Unavailable(QuorumError.OVERLOADED);
      upload = new Upload(id, pin(id), now);
      uploads.put(peer, upload);
    }
    upload.accessed = now;
    long total = upload.pin.length();
    if (position < 0 || position >= total || maxBytes < 1 || maxBytes > 256 * 1024)
      throw new Unavailable(QuorumError.INVALID_REQUEST);
    byte[] bytes = upload.pin.read(position, maxBytes);
    if (position + bytes.length == total) {
      uploads.remove(peer);
      upload.pin.close();
    }
    return new UploadChunk(id, position, total, bytes);
  }

  public synchronized int activeUploads() {
    return uploads.size();
  }

  /**
   * Reloads the retained set from the journal. Needed after another {@code SnapshotStore} instance
   * on the same root, such as the one used by snapshot install, appended a {@code SNAPSHOT_SET}.
   */
  public void refreshRetained() throws IOException {
    retained =
        new SnapshotStore(directory.getParent(), identity, files, state, maxBytes).retained();
  }

  @Override
  public synchronized void close() throws IOException {
    cancelDownload();
    for (var upload : uploads.values()) upload.pin.close();
    uploads.clear();
  }

  /**
   * Encodes the snapshot file bytes for {@code id}. The header layout is listed in Task 5 of
   * {@code docs/superpowers/plans/2026-09-28-metadata-quorum-phase-3.md}.
   *
   * @throws IOException if the image boundary differs from {@code id.endOffset()} or the encoding
   *     exceeds the configured limit
   */
  public byte[] encode(SnapshotId id, MetadataImage image) throws IOException {
    if (image.appliedOffset() != id.endOffset())
      throw new IOException("Snapshot image boundary mismatch");
    byte[] payload = MetadataImageCodec.encode(image);
    long length = 102L + payload.length;
    if (length > maxBytes) throw new IOException("Snapshot exceeds configured limit");
    byte[] bytes = new byte[(int) length];
    var out = ByteBuffer.wrap(bytes);
    out.putInt(0x51534e31)
        .putShort((short) 1)
        .putLong(length)
        .putLong(identity.clusterId().getMostSignificantBits())
        .putLong(identity.clusterId().getLeastSignificantBits())
        .put(identity.voterHash());
    id.writeTo(out);
    out.putInt(payload.length).put(payload);
    out.putInt(StateJournal.crc(bytes, 0, bytes.length - 4));
    return bytes;
  }

  /**
   * Decodes and fully validates snapshot file bytes: length bounds, header, cluster ID, voter set
   * hash, identity, payload size, checksum and image boundary.
   *
   * @throws IOException if any check fails, including malformed content
   */
  public MetadataImage decode(SnapshotId expected, byte[] bytes) throws IOException {
    if (bytes.length < 114 || bytes.length > Math.min(maxBytes, 512 + 128 * 281))
      throw new IOException("Invalid snapshot length");
    try {
      var in = ByteBuffer.wrap(bytes);
      if (in.getInt() != 0x51534e31 || in.getShort() != 1 || in.getLong() != bytes.length)
        throw new IOException("Invalid snapshot header");
      if (!new UUID(in.getLong(), in.getLong()).equals(identity.clusterId()))
        throw new IOException("Snapshot cluster mismatch");
      byte[] hash = new byte[32];
      in.get(hash);
      if (!Arrays.equals(hash, identity.voterHash()))
        throw new IOException("Snapshot voter mismatch");
      SnapshotId actual = SnapshotId.readFrom(in);
      if (!actual.equals(expected)) throw new IOException("Snapshot identity mismatch");
      int count = in.getInt();
      if (count < 12 || count != in.remaining() - 4)
        throw new IOException("Invalid snapshot payload size");
      if (ByteBuffer.wrap(bytes).getInt(bytes.length - 4)
          != StateJournal.crc(bytes, 0, bytes.length - 4))
        throw new IOException("Snapshot checksum mismatch");
      byte[] payload = new byte[count];
      in.get(payload);
      var image = MetadataImageCodec.decode(payload);
      if (image.appliedOffset() != expected.endOffset())
        throw new IOException("Snapshot boundary mismatch");
      return image;
    } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
      throw new IOException("Invalid snapshot", e);
    }
  }

  /**
   * Reads and validates the stored file for {@code id}.
   *
   * @throws IOException if the file is missing, too large, or fails {@link #decode}
   */
  public MetadataImage load(SnapshotId id) throws IOException {
    long length = Files.size(path(id));
    if (length > Math.min(maxBytes, 512 + 128 * 281))
      throw new IOException("Snapshot exceeds limit");
    return decode(id, Files.readAllBytes(path(id)));
  }

  /** Published snapshots, newest first; at most two. */
  public List<SnapshotId> retained() {
    return retained;
  }

  public Path path(SnapshotId id) {
    return directory.resolve(id.contentId() + ".snapshot");
  }

  /**
   * Validates {@code id} and opens it for reading. {@link #releaseObsolete} does not delete a
   * snapshot while any pin on it is open.
   */
  public synchronized Pin pin(SnapshotId id) throws IOException {
    load(id);
    FileChannel channel = FileChannel.open(path(id), READ);
    pins.merge(id, 1, Integer::sum);
    return new Pin(id, channel);
  }

  // A retained, generation-base, or pinned snapshot may still be read by recovery or an upload.
  /**
   * Deletes {@code .snapshot} files that are not retained, not the active generation's base
   * snapshot, and not pinned, then syncs the directory if anything was deleted. Files with other
   * names are left alone.
   */
  public synchronized void releaseObsolete() throws IOException {
    var referenced = new HashSet<UUID>();
    for (var id : retained) referenced.add(id.contentId());
    var base = GenerationStore.snapshotReference(state);
    if (base != null) referenced.add(base.contentId());
    for (var id : pins.keySet()) referenced.add(id.contentId());
    boolean deleted = false;
    try (var paths = Files.list(directory)) {
      for (var path : paths.toList()) {
        String name = path.getFileName().toString();
        if (!name.endsWith(".snapshot")) continue;
        UUID id;
        try {
          id = UUID.fromString(name.substring(0, name.length() - 9));
        } catch (IllegalArgumentException ignored) {
          continue;
        }
        if (!referenced.contains(id)) {
          Files.delete(path);
          deleted = true;
        }
      }
    }
    if (deleted) files.syncDirectory(directory);
  }

  /** An open read handle that protects one snapshot file from deletion until closed. */
  public final class Pin implements AutoCloseable {
    private final SnapshotId id;
    private final FileChannel channel;
    private boolean closed;

    private Pin(SnapshotId id, FileChannel channel) {
      this.id = id;
      this.channel = channel;
    }

    public long length() throws IOException {
      return channel.size();
    }

    /**
     * Reads up to {@code maxBytes} starting at {@code position}; shorter only at end of file.
     *
     * @throws IllegalArgumentException if the pin is closed, {@code maxBytes} is outside 1..256
     *     KiB, or {@code position} is outside the file
     */
    public byte[] read(long position, int maxBytes) throws IOException {
      if (closed
          || position < 0
          || maxBytes <= 0
          || maxBytes > 256 * 1024
          || position > channel.size())
        throw new IllegalArgumentException("Invalid snapshot chunk");
      var buffer = ByteBuffer.allocate((int) Math.min(maxBytes, channel.size() - position));
      while (buffer.hasRemaining()) {
        int n = channel.read(buffer, position);
        if (n <= 0) throw new IOException("Snapshot read stalled");
        position += n;
      }
      return buffer.array();
    }

    @Override
    public void close() throws IOException {
      synchronized (SnapshotStore.this) {
        if (closed) return;
        closed = true;
        channel.close();
        pins.computeIfPresent(id, (key, count) -> count == 1 ? null : count - 1);
      }
    }
  }
}

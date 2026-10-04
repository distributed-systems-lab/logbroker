package vn.huyqt.logbroker.controller.persistence;

import static java.nio.file.StandardOpenOption.*;

import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.storage.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/**
 * Owns root lock and persisted voting identity independently of log generations.
 *
 * <p>An open store holds an exclusive lock on {@code .lock} until {@link #close}, so at most one
 * process can use a controller root. The epoch and vote survive restart and a vote, once cast,
 * cannot change within its epoch. Not thread-safe.
 */
public final class QuorumStateStore implements AutoCloseable {
    private final Path root;
    private final ClusterIdentity identity;
    private final StateJournal journal;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private long epoch;
    private int votedFor = -1;

    private QuorumStateStore(
            Path root,
            ClusterIdentity identity,
            StateJournal journal,
            FileChannel channel,
            FileLock lock) {
        this.root = root;
        this.identity = identity;
        this.journal = journal;
        lockChannel = channel;
        this.lock = lock;
    }

    /**
     * Initializes an empty controller root: lock file, an empty initial generation, the snapshot
     * directory, a journal with epoch 0 / no vote, and finally {@code identity.bin}.
     *
     * <p>{@code identity.bin} is written last and {@link #open} requires it, so a crash part-way
     * through leaves a root that cannot be opened. It is still non-empty, so it must be cleared
     * before it can be formatted again.
     *
     * @throws IOException if {@code root} is non-empty or does not support strict durability
     */
    public static void format(Path root, ClusterIdentity identity, DurableFiles files)
            throws IOException {
        if (Files.exists(root))
            try (var children = Files.list(root)) {
                if (children.findAny().isPresent())
                    throw new IOException("Format requires empty directory");
            }
        Files.createDirectories(root);
        files.verifySupport(root);
        files.writeNew(root.resolve(".lock"), new byte[0]);
        UUID generation = UUID.randomUUID();
        Path logPath = root.resolve("generations").resolve(generation.toString()).resolve("log");
        Files.createDirectories(logPath);
        try (var log = PartitionLog.open(logPath, LogConfig.defaults())) {
            log.flush();
        }
        files.syncDirectory(logPath);
        files.syncDirectory(logPath.getParent());
        files.syncDirectory(root.resolve("generations"));
        Files.createDirectory(root.resolve("snapshots"));
        files.syncDirectory(root);
        try (var journal = StateJournal.open(root.resolve("quorum-state.journal"), files)) {
            journal.append(
                    StateJournal.HARD_STATE, ByteBuffer.allocate(12).putLong(0).putInt(-1).array());
            journal.append(
                    StateJournal.GENERATION,
                    ByteBuffer.allocate(25)
                            .putLong(generation.getMostSignificantBits())
                            .putLong(generation.getLeastSignificantBits())
                            .putLong(0)
                            .put((byte) 0)
                            .array());
        }
        files.writeNew(root.resolve("identity.bin"), encodeIdentity(identity));
        files.syncDirectory(root);
        if (root.toAbsolutePath().getParent() != null)
            files.syncDirectory(root.toAbsolutePath().getParent());
    }

    /**
     * Opens a formatted root, takes its lock and replays the latest epoch and vote.
     *
     * <p>{@code identity} must match the formatted identity byte for byte, including the voter set;
     * a controller cannot silently join a different cluster or membership.
     *
     * @throws IOException if the root is unformatted, locked by another owner, bound to a different
     *     identity, or its hard-state history is invalid
     */
    public static QuorumStateStore open(Path root, ClusterIdentity identity, DurableFiles files)
            throws IOException {
        if (!Files.isRegularFile(root.resolve("identity.bin"))
                || !Files.isRegularFile(root.resolve("quorum-state.journal")))
            throw new IOException("Controller storage is missing or not formatted");
        if (Files.size(root.resolve("identity.bin")) > 1024)
            throw new IOException("Controller identity exceeds bound");
        byte[] identityBytes = Files.readAllBytes(root.resolve("identity.bin"));
        if (!identity.equals(decodeIdentity(identityBytes))
                || !Arrays.equals(encodeIdentity(identity), identityBytes))
            throw new IOException("Controller identity, voter set or checksum mismatch");
        files.verifySupport(root);
        FileChannel channel = FileChannel.open(root.resolve(".lock"), READ, WRITE);
        FileLock lock = null;
        StateJournal journal = null;
        try {
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IOException("Controller directory locked", e);
            }
            if (lock == null) throw new IOException("Controller directory locked");
            journal = StateJournal.open(root.resolve("quorum-state.journal"), files);
            var store = new QuorumStateStore(root, identity, journal, channel, lock);
            for (var frame : journal.frames())
                if (frame.type() == StateJournal.HARD_STATE) {
                    byte[] payload = frame.payload();
                    if (payload.length != 12) throw new IOException("Invalid hard-state length");
                    var buffer = ByteBuffer.wrap(payload);
                    long next = buffer.getLong();
                    int vote = buffer.getInt();
                    store.validateVote(next, vote);
                    store.epoch = next;
                    store.votedFor = vote;
                }
            return store;
        } catch (IOException | RuntimeException e) {
            if (journal != null) journal.close();
            if (lock != null) lock.release();
            channel.close();
            if (e instanceof IllegalStateException)
                throw new IOException("Invalid hard-state history", e);
            throw e;
        }
    }

    // Epochs never go backwards, votes name a configured voter (or -1 for none), and a vote already
    // cast in the current epoch cannot be replaced. Shared by recovery and persistVote.
    private void validateVote(long next, int vote) {
        if (next < epoch
                || next < 0
                || vote < -1
                || vote != -1 && identity.voters().stream().noneMatch(v -> v.id() == vote)
                || next == epoch && votedFor != -1 && vote != votedFor)
            throw new IllegalStateException("Invalid vote transition");
    }

    /**
     * Durably records the epoch and vote before the in-memory state changes. Must complete before
     * the node acts on the vote, for example by granting it to a peer.
     *
     * @param next epoch to record; must not be lower than the current epoch
     * @param vote voter ID, or {@code -1} for no vote
     * @throws IllegalStateException if the transition is invalid (see class documentation)
     */
    public void persistVote(long next, int vote) throws IOException {
        validateVote(next, vote);
        if (next == epoch && vote == votedFor) return;
        journal.append(
                StateJournal.HARD_STATE,
                ByteBuffer.allocate(12).putLong(next).putInt(vote).array());
        epoch = next;
        votedFor = vote;
    }

    /** Decodes the explicitly versioned, bounded root identity; does not acquire its lock. */
    public static ClusterIdentity decodeIdentity(byte[] bytes) throws IOException {
        try {
            if (bytes.length < 38 || bytes.length > 1024)
                throw new IOException("Invalid identity length");
            var in = ByteBuffer.wrap(bytes);
            if (in.getInt() != 0x51494431) throw new IOException("Invalid identity magic");
            short format = in.getShort();
            if (format != 1 && format != 2
                    || in.getInt() != bytes.length
                    || in.getInt(bytes.length - 4) != StateJournal.crc(bytes, 0, bytes.length - 4))
                throw new IOException("Invalid identity format, length or checksum");
            UUID cluster = new UUID(in.getLong(), in.getLong());
            int node = in.getInt();
            short version = format == 1 ? 1 : in.getShort();
            if (version != format || in.getInt() != 3)
                throw new IOException("Invalid identity feature or voter count");
            var voters = new ArrayList<ClusterIdentity.Voter>();
            for (int i = 0; i < 3; i++) {
                int id = in.getInt(), length = in.getInt();
                if (length < 1 || length > 255 || length > in.remaining() - 8)
                    throw new IOException("Invalid voter host length");
                byte[] host = new byte[length];
                in.get(host);
                String name =
                        java.nio.charset.StandardCharsets.UTF_8
                                .newDecoder()
                                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                                .decode(ByteBuffer.wrap(host))
                                .toString();
                voters.add(new ClusterIdentity.Voter(id, name, in.getInt()));
            }
            if (in.remaining() != 4) throw new IOException("Trailing identity bytes");
            return new ClusterIdentity(cluster, node, voters, version);
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
            throw new IOException("Invalid controller identity", e);
        }
    }

    private static byte[] encodeIdentity(ClusterIdentity identity) {
        byte[] voters = identity.canonicalVoters();
        byte[] bytes = new byte[34 + voters.length + (identity.metadataVersion() == 2 ? 2 : 0)];
        var out =
                ByteBuffer.wrap(bytes)
                        .putInt(0x51494431)
                        .putShort(identity.metadataVersion())
                        .putInt(bytes.length);
        out.putLong(identity.clusterId().getMostSignificantBits())
                .putLong(identity.clusterId().getLeastSignificantBits())
                .putInt(identity.nodeId());
        if (identity.metadataVersion() == 2) out.putShort(identity.metadataVersion());
        out.put(voters);
        out.putInt(StateJournal.crc(bytes, 0, bytes.length - 4));
        return bytes;
    }

    public long epoch() {
        return epoch;
    }

    public int votedFor() {
        return votedFor;
    }

    public StateJournal journal() {
        return journal;
    }

    public Path root() {
        return root;
    }

    public ClusterIdentity identity() {
        return identity;
    }

    @Override
    public void close() throws IOException {
        try {
            journal.close();
        } finally {
            try {
                lock.release();
            } finally {
                lockChannel.close();
            }
        }
    }
}

package vn.huyqt.logbroker.controller.protocol;

import static vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;
import vn.huyqt.logbroker.broker.metadata.*;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.persistence.StateJournal;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

/**
 * Two-pass bounded decoder: preflight validates sizes without materializing batch/chunk payloads.
 *
 * <p>Encodes and decodes controller protocol v1 frames as specified in {@code
 * docs/controller-protocol-v1.md}. Byte arrays passed in and returned include the 4-byte length
 * prefix. Decoding is strict: it rejects unknown operations and versions, bad checksums, negative
 * offsets and epochs, out-of-range counts and lengths, invalid UTF-8 and trailing bytes, all as
 * {@link IOException}. Stateless and thread-safe.
 */
public final class QuorumCodec {
  private QuorumCodec() {}

  /** Exact v1 wire size without materializing batch/chunk payloads. */
  public static long encodedSize(Frame frame) {
    // 4-byte length prefix plus the 69-byte minimum frame: 65-byte envelope and 4-byte CRC.
    long bytes = 73;
    var message = frame.message();
    if (message instanceof Reply reply) {
      bytes += 18 + utf8(reply.meta().message());
      if (reply.meta().error() != QuorumError.NONE) return bytes;
    }
    return bytes
        + switch (message) {
          case Vote ignored -> 24L;
          case BeginQuorumEpoch ignored -> 8L;
          case EndQuorumEpoch ignored -> 8L;
          case QuorumFetch ignored -> 40L;
          case FetchSnapshot ignored -> 52L;
          case DescribeQuorum ignored -> 0L;
          case ReadLocalMetadata ignored -> 0L;
          case CreateTopic create -> 12L + utf8(create.name());
          case ReadMetadata ignored -> 4L;
          case VoteReply ignored -> 1L;
          case EpochReply ignored -> 0L;
          case Failure ignored -> 0L;
          case CreateTopicReply ignored -> 16L;
          case FetchSnapshotReply reply -> 56L + reply.chunkLength();
          case DescribeQuorumReply reply ->
              82L + 12L * reply.status().durableMatches().size() + utf8(reply.status().failure());
          // Topic names are ASCII-only, so String.length() equals their UTF-8 size.
          case MetadataReply reply ->
              37L
                  + reply.view().topics().stream()
                      .mapToLong(topic -> 30L + topic.name().length())
                      .sum();
          case QuorumFetchReply reply ->
              16L
                  + switch (reply.payload()) {
                    case Divergence ignored -> 17L;
                    case SnapshotRequired ignored -> 33L;
                    case FetchData data ->
                        5L
                            + data.batches().stream()
                                .mapToLong(
                                    batch ->
                                        16L
                                            + batch.entries().stream()
                                                .mapToLong(
                                                    entry ->
                                                        4L + QuorumEntryCodec.encodedSize(entry))
                                                .sum())
                                .sum();
                  };
        };
  }

  /**
   * Conservative outbound memory charge for {@code frame}: eight times its encoded size plus 64
   * bytes per batch, entry or topic. See {@code docs/controller-configuration.md} for how the
   * transport holds this reservation.
   *
   * @throws ArithmeticException on overflow
   */
  public static long outboundCharge(Frame frame) {
    long elements = 0;
    if (frame.message() instanceof QuorumFetchReply reply
        && reply.payload() instanceof FetchData data)
      for (var batch : data.batches()) elements += 1L + batch.entries().size();
    else if (frame.message() instanceof MetadataReply reply)
      elements = reply.view().topics().size();
    return Math.addExact(
        Math.multiplyExact(8, encodedSize(frame)), Math.multiplyExact(64, elements));
  }

  private static int utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  /**
   * Encodes {@code frame}, including the length prefix and trailing CRC32C. Callers enforce size
   * limits; encoding itself does not check the configured frame maximum.
   */
  public static byte[] encode(Frame frame) {
    try {
      var bytes = new ByteArrayOutputStream();
      var out = new DataOutputStream(bytes);
      out.writeShort(frame.operation());
      out.writeShort(1);
      out.writeByte(frame.response() ? 1 : 0);
      uuid(out, frame.clusterId());
      out.writeInt(frame.senderId());
      out.writeLong(frame.requestId());
      out.write(frame.voterHash());
      writeBody(out, frame.message());
      out.flush();
      byte[] payload = bytes.toByteArray();
      return ByteBuffer.allocate(payload.length + 8)
          .putInt(payload.length + 4)
          .put(payload)
          .putInt(StateJournal.crc(payload, 0, payload.length))
          .array();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeBody(DataOutputStream out, Message message) throws IOException {
    if (message instanceof Reply reply) {
      var m = reply.meta();
      out.writeShort(m.error().number());
      string(out, m.message());
      out.writeLong(m.epoch());
      out.writeInt(m.leaderId());
      if (m.error() != QuorumError.NONE) return;
    }
    switch (message) {
      case Vote r -> {
        out.writeLong(r.epoch());
        out.writeLong(r.lastEpoch());
        out.writeLong(r.end());
      }
      case BeginQuorumEpoch r -> out.writeLong(r.epoch());
      case EndQuorumEpoch r -> out.writeLong(r.epoch());
      case QuorumFetch r -> {
        out.writeLong(r.epoch());
        out.writeLong(r.end());
        out.writeLong(r.lastEpoch());
        out.writeInt(r.maxBytes());
        out.writeInt(r.maxWaitMs());
        out.writeLong(r.challenge());
      }
      case FetchSnapshot r -> {
        out.writeLong(r.epoch());
        snapshot(out, r.id());
        out.writeLong(r.position());
        out.writeInt(r.maxBytes());
      }
      case DescribeQuorum ignored -> {}
      case ReadLocalMetadata ignored -> {}
      case CreateTopic r -> {
        string(out, r.name());
        out.writeInt(r.partitions());
        out.writeInt(r.timeoutMs());
      }
      case ReadMetadata r -> out.writeInt(r.timeoutMs());
      case VoteReply r -> out.writeBoolean(r.granted());
      case EpochReply ignored -> {}
      case QuorumFetchReply r -> {
        out.writeLong(r.challenge());
        out.writeLong(r.commit());
        switch (r.payload()) {
          case FetchData data -> {
            out.writeByte(0);
            out.writeInt(data.batches().size());
            for (var batch : data.batches()) writeBatch(out, batch);
          }
          case Divergence d -> {
            out.writeByte(1);
            out.writeLong(d.commonEpoch());
            out.writeLong(d.commonEnd());
          }
          case SnapshotRequired s -> {
            out.writeByte(2);
            snapshot(out, s.id());
          }
        }
      }
      case FetchSnapshotReply r -> {
        snapshot(out, r.id());
        out.writeLong(r.position());
        out.writeLong(r.totalLength());
        byte[] chunk = r.chunk();
        blob(out, chunk);
        out.writeInt(StateJournal.crc(chunk, 0, chunk.length));
      }
      case DescribeQuorumReply r -> {
        var s = r.status();
        out.writeInt(s.nodeId());
        // Role is encoded by ordinal, so QuorumStatus.Role order is part of the wire format.
        out.writeByte(s.role().ordinal());
        out.writeLong(s.epoch());
        out.writeInt(s.leaderId());
        uuid(out, s.generation());
        out.writeLong(s.logEnd());
        out.writeLong(s.durableEnd());
        out.writeLong(s.commit());
        out.writeLong(s.applied());
        out.writeLong(s.snapshotEnd());
        out.writeBoolean(s.ready());
        out.writeInt(s.durableMatches().size());
        // Sorted by voter ID so equal statuses encode to identical bytes.
        for (var e : new TreeMap<>(s.durableMatches()).entrySet()) {
          out.writeInt(e.getKey());
          out.writeLong(e.getValue());
        }
        string(out, s.failure());
      }
      case CreateTopicReply r -> uuid(out, r.topicId());
      case MetadataReply r -> {
        var v = r.view();
        out.writeByte(v.consistency().ordinal());
        out.writeInt(v.nodeId());
        out.writeLong(v.epoch());
        out.writeInt(v.leaderId());
        out.writeLong(v.commit());
        out.writeLong(v.applied());
        out.writeInt(v.topics().size());
        for (var topic : v.topics()) blob(out, MetadataEventCodec.encode(topic));
      }
      case Failure ignored -> {}
    }
  }

  private static void writeBatch(DataOutputStream out, QuorumBatch batch) throws IOException {
    var bytes = new ByteArrayOutputStream();
    var b = new DataOutputStream(bytes);
    b.writeLong(batch.baseOffset());
    b.writeInt(batch.entries().size());
    for (var entry : batch.entries()) blob(b, QuorumEntryCodec.encode(entry));
    b.flush();
    byte[] payload = bytes.toByteArray();
    out.write(payload);
    out.writeInt(StateJournal.crc(payload, 0, payload.length));
  }

  private static void uuid(DataOutputStream out, UUID id) throws IOException {
    out.writeLong(id.getMostSignificantBits());
    out.writeLong(id.getLeastSignificantBits());
  }

  private static void snapshot(DataOutputStream out, SnapshotId id) throws IOException {
    out.writeLong(id.endOffset());
    out.writeLong(id.lastEpoch());
    uuid(out, id.contentId());
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    blob(out, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void blob(DataOutputStream out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * Validates a whole frame, including checksums and limits from {@code config}, without
   * materializing batch entries, chunks or topics.
   *
   * @return memory to reserve before {@link #decode}: twice the frame length plus 64 bytes per
   *     decoded array element
   * @throws IOException if the frame is invalid
   */
  public static long preflight(byte[] bytes, ControllerConfig config) throws IOException {
    var reader = new Reader(bytes, config, false);
    reader.frame();
    return Math.addExact(2L * bytes.length, 64L * reader.elements);
  }

  /**
   * Decodes a frame after running {@link #preflight} on it. Identity (cluster, sender, voter hash)
   * is parsed but not checked against membership; callers such as the transport and {@link
   * vn.huyqt.logbroker.controller.client.ControllerClient} check it.
   *
   * @throws IOException if the frame is invalid
   */
  public static Frame decode(byte[] bytes, ControllerConfig config) throws IOException {
    preflight(bytes, config);
    return new Reader(bytes, config, true).frame();
  }

  private static final class Reader {
    private final byte[] bytes;
    private final ByteBuffer in;
    private final ControllerConfig config;
    private final boolean materialize;
    private long elements;

    Reader(byte[] bytes, ControllerConfig config, boolean materialize) throws IOException {
      if (bytes == null || bytes.length < 73 || bytes.length > config.maxFrameBytes() + 4)
        throw new IOException("Invalid quorum frame size");
      this.bytes = bytes;
      this.config = config;
      this.materialize = materialize;
      in = ByteBuffer.wrap(bytes);
    }

    Frame frame() throws IOException {
      try {
        if (in.getInt() != bytes.length - 4) throw new IOException("Frame length mismatch");
        short op = in.getShort();
        if (op < 101 || op > 109) throw new IOException("Unsupported operation");
        if (in.getShort() != 1) throw new IOException("Unsupported quorum version");
        boolean reply = bool();
        UUID cluster = uuid();
        int sender = in.getInt();
        long request = in.getLong();
        byte[] hash = new byte[32];
        in.get(hash);
        // Verify the frame CRC before interpreting any body field; the limit then hides the CRC
        // so body parsing cannot read it and trailing-byte detection is exact.
        if (in.getInt(bytes.length - 4) != StateJournal.crc(bytes, 4, bytes.length - 8))
          throw new IOException("Frame checksum mismatch");
        in.limit(bytes.length - 4);
        Message body = body(op, reply);
        if (in.hasRemaining()) throw new IOException("Trailing quorum bytes");
        return new Frame(op, reply, cluster, sender, request, hash, body);
      } catch (BufferUnderflowException | IllegalArgumentException | ArithmeticException e) {
        throw new IOException("Invalid quorum payload", e);
      }
    }

    private Message body(short op, boolean reply) throws IOException {
      if (!reply)
        return switch (op) {
          case 101 -> new Vote(nonnegative(), nonnegative(), nonnegative());
          case 102 -> new BeginQuorumEpoch(nonnegative());
          case 103 -> new EndQuorumEpoch(nonnegative());
          case 104 ->
              new QuorumFetch(
                  nonnegative(),
                  nonnegative(),
                  nonnegative(),
                  positive(config.fetchMaxBytes()),
                  range(0, (int) config.fetchIdleWait().toMillis()),
                  nonnegative());
          case 105 ->
              new FetchSnapshot(
                  nonnegative(), snapshot(), nonnegative(), positive(config.snapshotChunkBytes()));
          case 106 -> new DescribeQuorum();
          case 107 ->
              new CreateTopic(
                  string(249),
                  positive(config.maxPartitions()),
                  positive((int) config.adminTimeout().toMillis()));
          case 108 -> new ReadMetadata(positive((int) config.adminTimeout().toMillis()));
          case 109 -> new ReadLocalMetadata();
          default -> throw new IOException("Unknown operation");
        };
      var meta =
          new ReplyMeta(
              QuorumError.fromNumber(in.getShort()), string(512), nonnegative(), in.getInt());
      if (meta.error() != QuorumError.NONE) return new Failure(meta);
      return switch (op) {
        case 101 -> new VoteReply(meta, bool());
        case 102, 103 -> new EpochReply(meta);
        case 104 -> {
          long challenge = nonnegative(), commit = nonnegative();
          int kind = in.get() & 255;
          FetchPayload payload =
              switch (kind) {
                case 0 -> new FetchData(batches());
                case 1 -> new Divergence(nonnegative(), nonnegative());
                case 2 -> new SnapshotRequired(snapshot());
                default -> throw new IOException("Invalid Fetch payload");
              };
          yield new QuorumFetchReply(meta, challenge, commit, payload);
        }
        case 105 -> {
          SnapshotId id = snapshot();
          long position = nonnegative(), total = nonnegative();
          if (total > config.snapshotMaxBytes() || position > total)
            throw new IOException("Invalid snapshot transfer length");
          int n = length(0, config.snapshotChunkBytes());
          if (n > total - position) throw new IOException("Snapshot chunk beyond total");
          int start = in.position();
          byte[] chunk = take(n);
          int crc = in.getInt();
          if (crc != StateJournal.crc(bytes, start, n))
            throw new IOException("Snapshot chunk checksum mismatch");
          yield new FetchSnapshotReply(meta, id, position, total, chunk);
        }
        case 106 -> new DescribeQuorumReply(meta, status());
        case 107 -> new CreateTopicReply(meta, uuid());
        case 108, 109 -> new MetadataReply(meta, view());
        default -> throw new IOException("Invalid reply op");
      };
    }

    private List<QuorumBatch> batches() throws IOException {
      int start = in.position();
      int count = count(config.fetchMaxBytes() / 27, 27);
      var batches = new ArrayList<QuorumBatch>();
      for (int i = 0; i < count; i++) {
        int begin = in.position();
        long base = nonnegative();
        int records = count(config.logConfig().maxBatchBytes() / 15, 15);
        if (records == 0) throw new IOException("Empty replicated batch");
        var entries = new ArrayList<QuorumEntry>();
        // Size the batch as the follower will store it (30-byte storage header, 20 bytes per
        // keyless record) so an accepted batch also fits the local log's max batch size.
        long storageBytes = 30;
        for (int j = 0; j < records; j++) {
          int n = length(11, config.logConfig().maxBatchBytes());
          storageBytes = Math.addExact(storageBytes, 20L + n);
          byte[] entry = take(n);
          if (materialize) entries.add(QuorumEntryCodec.decode(entry));
        }
        int end = in.position();
        int crc = in.getInt();
        if (crc != StateJournal.crc(bytes, begin, end - begin))
          throw new IOException("Batch checksum mismatch");
        if (storageBytes > config.logConfig().maxBatchBytes()
            || end + 4 - begin > config.logConfig().maxBatchBytes())
          throw new IOException("Replicated batch too large");
        if (materialize) batches.add(new QuorumBatch(base, entries));
      }
      if (in.position() - start > config.fetchMaxBytes() + 4)
        throw new IOException("Fetch response budget exceeded");
      if (materialize)
        for (int i = 1; i < batches.size(); i++)
          if (batches.get(i).baseOffset() != batches.get(i - 1).nextOffset())
            throw new IOException("Fetch batch gap");
      return List.copyOf(batches);
    }

    private QuorumStatus status() throws IOException {
      int node = in.getInt(), role = in.get() & 255;
      if (role >= QuorumStatus.Role.values().length) throw new IOException("Invalid role");
      long epoch = nonnegative();
      int leader = in.getInt();
      UUID generation = uuid();
      long end = nonnegative(),
          durable = nonnegative(),
          commit = nonnegative(),
          applied = nonnegative(),
          snapshot = nonnegative();
      boolean ready = bool();
      int count = count(3, 12);
      var matches = new HashMap<Integer, Long>();
      for (int i = 0; i < count; i++) {
        int id = in.getInt();
        long offset = nonnegative();
        if (matches.put(id, offset) != null) throw new IOException("Duplicate progress voter");
      }
      if (snapshot > applied || applied > commit || commit > durable || durable > end)
        throw new IOException("Invalid offset progress");
      return new QuorumStatus(
          node,
          QuorumStatus.Role.values()[role],
          epoch,
          leader,
          generation,
          end,
          durable,
          commit,
          applied,
          snapshot,
          ready,
          matches,
          string(512));
    }

    private MetadataView view() throws IOException {
      int consistency = in.get() & 255;
      if (consistency > 1) throw new IOException("Invalid consistency");
      int node = in.getInt();
      long epoch = nonnegative();
      int leader = in.getInt();
      long commit = nonnegative(), applied = nonnegative();
      if (applied > commit) throw new IOException("Apply exceeds commit");
      int count = count(config.maxTopics(), 31);
      var topics = new ArrayList<TopicCreated>();
      for (int i = 0; i < count; i++) {
        int n = length(27, 275);
        byte[] entry = take(n);
        if (materialize) topics.add(MetadataEventCodec.decode(entry));
      }
      return new MetadataView(
          Consistency.values()[consistency], node, epoch, leader, commit, applied, topics);
    }

    // Bounds an array count by its cap and by remaining bytes at min bytes per element before
    // anything is allocated for it.
    private int count(int max, int min) throws IOException {
      int n = in.getInt();
      if (n < 0 || n > max || n > in.remaining() / min)
        throw new IOException("Invalid array count");
      elements = Math.addExact(elements, n);
      return n;
    }

    private int length(int min, int max) throws IOException {
      int n = in.getInt();
      if (n < min || n > max || n > in.remaining()) throw new IOException("Invalid blob length");
      return n;
    }

    private byte[] take(int n) {
      if (!materialize) {
        in.position(in.position() + n);
        return new byte[0];
      }
      byte[] result = new byte[n];
      in.get(result);
      return result;
    }

    private String string(int max) throws IOException {
      int n = length(0, max);
      var slice = in.slice();
      slice.limit(n);
      in.position(in.position() + n);
      try {
        return StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(slice)
            .toString();
      } catch (CharacterCodingException e) {
        throw new IOException("Invalid UTF-8", e);
      }
    }

    private long nonnegative() throws IOException {
      long n = in.getLong();
      if (n < 0) throw new IOException("Negative offset or epoch");
      return n;
    }

    private int positive(int max) throws IOException {
      return range(1, max);
    }

    private int range(int min, int max) throws IOException {
      int n = in.getInt();
      if (n < min || n > max) throw new IOException("Integer out of range");
      return n;
    }

    private boolean bool() throws IOException {
      int n = in.get() & 255;
      if (n > 1) throw new IOException("Invalid boolean");
      return n == 1;
    }

    private UUID uuid() {
      return new UUID(in.getLong(), in.getLong());
    }

    private SnapshotId snapshot() {
      return SnapshotId.readFrom(in);
    }
  }
}

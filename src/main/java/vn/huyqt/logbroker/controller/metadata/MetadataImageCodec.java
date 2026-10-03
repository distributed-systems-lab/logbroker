package vn.huyqt.logbroker.controller.metadata;

import java.io.*;
import java.util.ArrayList;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CodingErrorAction;
import java.nio.ByteBuffer;
import vn.huyqt.logbroker.broker.metadata.MetadataEventCodec;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

/**
 * Binary form of a {@link MetadataImage}: applied offset i64, topic count i32, then each topic as
 * a length-prefixed {@link MetadataEventCodec} event, big-endian. This is the image payload
 * carried inside snapshot files.
 */
public final class MetadataImageCodec {
  private MetadataImageCodec() {}

  public static byte[] encode(MetadataImage image) {
    if (image.metadataVersion() != 1) throw new IllegalArgumentException("Use explicit v2 image encoder");
    try {
      var bytes = new ByteArrayOutputStream();
      var out = new DataOutputStream(bytes);
      out.writeLong(image.appliedOffset());
      out.writeInt(image.topics().size());
      for (var topic : image.topics()) {
        byte[] entry = MetadataEventCodec.encode(topic);
        out.writeInt(entry.length);
        out.write(entry);
      }
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new UncheckedIOException(impossible);
    }
  }

  /** Encodes a complete v2 image in canonical broker/UUID/partition order. */
  public static byte[] encodeV2(MetadataImage image, MetadataLimits limits) {
    if (image.metadataVersion() == 1) throw new IllegalArgumentException("Legacy image");
    image.validate(limits);
    try {
      var bytes = new ByteArrayOutputStream();
      var out = new DataOutputStream(bytes);
      out.writeInt(0x4d494d32);
      out.writeShort(2);
      out.writeShort(image.metadataVersion());
      out.writeLong(image.appliedOffset());
      out.writeInt(image.brokers().size());
      for (var view : image.brokers().values()) {
        var r = view.registration();
        out.writeInt(r.session().brokerId());
        uuid(out, r.session().storageId());
        uuid(out, r.session().incarnationId());
        out.writeLong(r.session().brokerEpoch());
        string(out, r.endpoint().host());
        out.writeInt(r.endpoint().port());
        out.writeShort(r.minVersion());
        out.writeShort(r.maxVersion());
        out.writeByte(view.fenced() ? 1 : 0);
        out.writeLong(view.stateOffset());
      }
      var topics = image.topics().stream().sorted(Comparator.comparing(
          t -> new MetadataImage.PartitionKey(t.id(), 0))).toList();
      out.writeInt(topics.size());
      for (var topic : topics) {
        uuid(out, topic.id());
        string(out, topic.name());
        out.writeInt(topic.partitions());
      }
      out.writeInt(image.partitions().size());
      for (var p : image.partitions().values()) {
        uuid(out, p.topicId());
        out.writeInt(p.partitionId());
        out.writeInt(1);
        out.writeInt(p.leaderId());
        out.writeInt(p.leaderId());
        out.writeLong(p.leaderEpoch());
        out.writeLong(p.partitionEpoch());
      }
      if (bytes.size() > limits.maxImageBytes()) throw new IllegalArgumentException("Image byte budget");
      return bytes.toByteArray();
    } catch (IOException impossible) { throw new UncheckedIOException(impossible); }
  }

  /** Exact encoded bytes without allocating an image payload. */
  public static long encodedSize(MetadataImage image) {
    if (image.metadataVersion() == 1)
      return 12L + image.topics().stream().mapToLong(t -> 30L + t.name().length()).sum();
    return 28L + image.brokers().values().stream().mapToLong(v ->
        65L + v.registration().endpoint().host().getBytes(StandardCharsets.UTF_8).length).sum()
        + image.topics().stream().mapToLong(t -> 24L + t.name().length()).sum()
        + 48L * image.partitions().size();
  }

  /** Parses bounded v2 counts and validates all references before returning an image. */
  public static MetadataImage decodeV2(byte[] bytes, MetadataLimits limits) throws IOException {
    if (bytes == null || bytes.length < 28 || bytes.length > limits.maxImageBytes())
      throw new IOException("Invalid image size");
    try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      if (in.readInt() != 0x4d494d32 || in.readShort() != 2) throw new IOException("Invalid image schema");
      short feature = in.readShort();
      if (feature != 0 && feature != 2) throw new IOException("Invalid cluster feature level");
      long offset = in.readLong();
      int count = count(in, limits.maxBrokers(), 66);
      var brokers = new TreeMap<Integer, MetadataImage.BrokerRegistrationView>();
      for (int i = 0; i < count; i++) {
        var session = new ClusterRecords.Session(in.readInt(), uuid(in), uuid(in), in.readLong());
        var endpoint = new ClusterRecords.Endpoint(string(in, 255), in.readInt());
        var r = new ClusterRecords.BrokerRegistration(session, endpoint, in.readShort(), in.readShort());
        int fenced = in.readUnsignedByte();
        if (fenced > 1) throw new IOException("Invalid fenced flag");
        var view = new MetadataImage.BrokerRegistrationView(r, fenced == 1, in.readLong());
        if (brokers.put(session.brokerId(), view) != null) throw new IOException("Duplicate broker");
      }
      count = count(in, limits.maxTopics(), 25);
      var topics = new ArrayList<TopicCreated>(count);
      long total = 0;
      for (int i = 0; i < count; i++) {
        var topic = new TopicCreated(uuid(in), string(in, 249), in.readInt());
        total += topic.partitions();
        if (total > limits.maxPartitions()) throw new IOException("Partition capacity");
        topics.add(topic);
      }
      count = count(in, limits.maxPartitions(), 48);
      var partitions = new TreeMap<MetadataImage.PartitionKey, ClusterRecords.PartitionRecord>();
      for (int i = 0; i < count; i++) {
        UUID id = uuid(in);
        int p = in.readInt();
        if (in.readInt() != 1) throw new IOException("Invalid replica count");
        var record = new ClusterRecords.PartitionRecord(id, p, List.of(in.readInt()), in.readInt(), in.readLong(), in.readLong());
        if (partitions.put(new MetadataImage.PartitionKey(id, p), record) != null)
          throw new IOException("Duplicate partition");
      }
      if (in.available() != 0) throw new IOException("Trailing image bytes");
      var image = new MetadataImage(offset, topics, feature, brokers, partitions);
      image.validate(limits);
      return image;
    } catch (IllegalArgumentException e) { throw new IOException("Invalid cluster image", e); }
  }

  private static int count(DataInputStream in, int max, int minBytes) throws IOException {
    int count = in.readInt();
    if (count < 0 || count > max || count > in.available() / minBytes) throw new IOException("Invalid image count");
    return count;
  }

  /** Structural preflight without materializing images; returns array elements for memory charging. */
  public static long preflightV2(ByteBuffer source, MetadataLimits limits) throws IOException {
    var in = source.slice();
    if (in.remaining() < 28 || in.remaining() > limits.maxImageBytes()) throw new IOException("Invalid image size");
    try {
      if (in.getInt() != 0x4d494d32 || in.getShort() != 2) throw new IOException("Invalid image schema");
      int feature = in.getShort();
      if (feature != 0 && feature != 2 || in.getLong() < 0) throw new IOException("Invalid feature or offset");
      int brokers = checkedCount(in, limits.maxBrokers(), 66);
      for (int i = 0; i < brokers; i++) {
        skip(in, 44); checkedString(in, 255); skip(in, 8);
        if ((in.get() & 255) > 1) throw new IOException("Invalid fence boolean");
        skip(in, 8);
      }
      int topics = checkedCount(in, limits.maxTopics(), 25);
      long total = 0;
      for (int i = 0; i < topics; i++) {
        skip(in, 16); checkedString(in, 249);
        int partitions = in.getInt();
        if (partitions < 1 || (total += partitions) > limits.maxPartitions()) throw new IOException("Invalid partition total");
      }
      int partitions = checkedCount(in, limits.maxPartitions(), 48);
      for (int i = 0; i < partitions; i++) {
        skip(in, 20);
        if (in.getInt() != 1) throw new IOException("Invalid replica count");
        skip(in, 24);
      }
      if (in.hasRemaining() || feature == 0 && brokers + topics + partitions != 0)
        throw new IOException("Trailing or uninitialized image");
      return (long) brokers + topics + partitions;
    } catch (java.nio.BufferUnderflowException e) { throw new IOException("Truncated image", e); }
  }
  private static int checkedCount(ByteBuffer in, int max, int min) throws IOException {
    int n = in.getInt();
    if (n < 0 || n > max || n > in.remaining() / min) throw new IOException("Invalid count");
    return n;
  }
  private static void skip(ByteBuffer in, int n) throws IOException {
    if (n > in.remaining()) throw new IOException("Truncated image");
    in.position(in.position() + n);
  }
  private static void checkedString(ByteBuffer in, int max) throws IOException {
    int n = in.getInt();
    if (n < 1 || n > max || n > in.remaining()) throw new IOException("Invalid string length");
    var slice = in.slice(); slice.limit(n);
    StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(slice);
    skip(in, n);
  }
  private static void uuid(DataOutputStream out, UUID id) throws IOException {
    out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits());
  }
  private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }
  private static void string(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length); out.write(bytes);
  }
  private static String string(DataInputStream in, int max) throws IOException {
    int n = in.readInt();
    if (n < 1 || n > max || n > in.available()) throw new IOException("Invalid string length");
    return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(in.readNBytes(n))).toString();
  }

  /**
   * Decodes an image, consuming every byte. Counts and lengths are checked against the remaining
   * input before anything is allocated for them.
   *
   * @throws IOException if the encoding is malformed or the image violates {@link MetadataImage}
   *     limits
   */
  public static MetadataImage decode(byte[] bytes) throws IOException {
    if (bytes == null || bytes.length < 12 || bytes.length > 64 * 1024 * 1024)
      throw new IOException("Invalid image size");
    try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      long offset = in.readLong();
      int count = in.readInt();
      // 31 bytes is the smallest topic element: a 4-byte length plus a 27-byte event.
      if (count < 0 || count > 128 || count > in.available() / 31)
        throw new IOException("Invalid topic count");
      var topics = new ArrayList<TopicCreated>(count);
      for (int i = 0; i < count; i++) {
        int n = in.readInt();
        // Event size is 26 bytes plus the 1..249-byte topic name.
        if (n < 27 || n > 275 || n > in.available()) throw new IOException("Invalid event length");
        topics.add(MetadataEventCodec.decode(in.readNBytes(n)));
      }
      if (in.available() != 0) throw new IOException("Trailing image bytes");
      return new MetadataImage(offset, topics);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid image", e);
    }
  }
}

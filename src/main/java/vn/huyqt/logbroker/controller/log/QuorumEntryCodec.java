package vn.huyqt.logbroker.controller.log;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords;
import vn.huyqt.logbroker.controller.metadata.MetadataLimits;
import vn.huyqt.logbroker.broker.metadata.MetadataEventCodec;

/**
 * Versioned encoding of one {@link QuorumEntry}, used both as a storage record value and inside
 * replicated batches. The layout is specified in {@code docs/controller-protocol-v1.md}.
 */
public final class QuorumEntryCodec {
  private QuorumEntryCodec() {}

  /** Returns {@code encode(entry).length} without allocating the encoding. */
  public static int encodedSize(QuorumEntry entry) {
    return switch (entry) {
      case QuorumEntry.LeaderChange ignored -> 15;
      case QuorumEntry.ReadBarrier ignored -> 11;
      // Topic names are restricted to ASCII, so the character count equals the UTF-8 length.
      case QuorumEntry.Topic topic -> 37 + topic.event().name().length();
      case QuorumEntry.FeatureLevel ignored -> 13;
      case QuorumEntry.BrokerRegistration broker -> 67 + broker.event().endpoint().host().getBytes(StandardCharsets.UTF_8).length;
      case QuorumEntry.BrokerState ignored -> 24;
      case QuorumEntry.TopicRecord topic -> 35 + topic.event().name().length();
      case QuorumEntry.PartitionRecord ignored -> 59;
    };
  }

  public static byte[] encode(QuorumEntry entry) {
    return encode(entry, (short) (isCluster(entry) ? 2 : 1));
  }

  /** Encodes the selected schema; v1 cannot contain cluster metadata kinds. */
  public static byte[] encode(QuorumEntry entry, short version) {
    if (version != 1 && version != 2 || version == 1 && isCluster(entry))
      throw new IllegalArgumentException("Unsupported entry schema");
    if (isCluster(entry)) {
      var out = ByteBuffer.allocate(encodedSize(entry));
      out.putShort(version).put(clusterKind(entry)).putLong(entry.epoch());
      switch (entry) {
        case QuorumEntry.FeatureLevel feature -> out.putShort(feature.event().level());
        case QuorumEntry.BrokerRegistration broker -> {
          var r = broker.event();
          out.putInt(r.session().brokerId());
          putUuid(out, r.session().storageId());
          putUuid(out, r.session().incarnationId());
          out.putLong(r.session().brokerEpoch());
          putString(out, r.endpoint().host());
          out.putInt(r.endpoint().port()).putShort(r.minVersion()).putShort(r.maxVersion());
        }
        case QuorumEntry.BrokerState broker -> out.putInt(broker.event().brokerId())
            .putLong(broker.event().brokerEpoch()).put((byte) (broker.event().fenced() ? 1 : 0));
        case QuorumEntry.TopicRecord topic -> {
          putUuid(out, topic.event().topicId());
          putString(out, topic.event().name());
          out.putInt(topic.event().partitions());
        }
        case QuorumEntry.PartitionRecord partition -> {
          var p = partition.event();
          putUuid(out, p.topicId());
          out.putInt(p.partitionId()).putInt(p.replicas().size());
          p.replicas().forEach(out::putInt);
          out.putInt(p.leaderId()).putLong(p.leaderEpoch()).putLong(p.partitionEpoch());
        }
        default -> throw new IllegalArgumentException("Not cluster metadata");
      }
      return out.array();
    }
    byte[] payload;
    byte kind;
    switch (entry) {
      case QuorumEntry.LeaderChange leader -> {
        kind = 1;
        payload = ByteBuffer.allocate(4).putInt(leader.leaderId()).array();
      }
      case QuorumEntry.Topic topic -> {
        kind = 2;
        payload = MetadataEventCodec.encode(topic.event());
      }
      case QuorumEntry.ReadBarrier ignored -> {
        kind = 3;
        payload = new byte[0];
      }
      default -> throw new IllegalArgumentException("Not legacy metadata");
    }
    return ByteBuffer.allocate(11 + payload.length)
        .putShort(version)
        .put(kind)
        .putLong(entry.epoch())
        .put(payload)
        .array();
  }

  /**
   * Decodes one entry, consuming every byte.
   *
   * @throws IOException if the length, version or kind is invalid, the payload is truncated or
   *     has trailing bytes, or a decoded field fails validation
   */
  public static QuorumEntry decode(byte[] bytes) throws IOException {
    return decode(bytes, MetadataLimits.defaults());
  }

  public static QuorumEntry decode(byte[] bytes, MetadataLimits limits) throws IOException {
    // No entry can exceed the 1 MiB metadata batch limit.
    if (bytes == null || bytes.length < 11 || bytes.length > 1024 * 1024)
      throw new IOException("Invalid entry length");
    try {
      var in = ByteBuffer.wrap(bytes);
      short version = in.getShort();
      if (version != 1 && version != 2) throw new IOException("Unsupported entry version");
      byte kind = in.get();
      long epoch = in.getLong();
      if (version == 1 && kind > 3) throw new IOException("Invalid v1 entry kind");
      QuorumEntry result =
          switch (kind) {
            case 1 -> new QuorumEntry.LeaderChange(epoch, in.getInt());
            case 2 -> {
              byte[] payload = new byte[in.remaining()];
              in.get(payload);
              yield new QuorumEntry.Topic(epoch, MetadataEventCodec.decode(payload));
            }
            case 3 -> new QuorumEntry.ReadBarrier(epoch);
            case 4 -> new QuorumEntry.FeatureLevel(epoch, new ClusterRecords.FeatureLevel(in.getShort()));
            case 5 -> {
              var session = new ClusterRecords.Session(in.getInt(), uuid(in), uuid(in), in.getLong());
              var endpoint = new ClusterRecords.Endpoint(string(in, 255), in.getInt());
              yield new QuorumEntry.BrokerRegistration(epoch, new ClusterRecords.BrokerRegistration(
                  session, endpoint, in.getShort(), in.getShort()));
            }
            case 6 -> {
              int id = in.getInt();
              long brokerEpoch = in.getLong();
              byte fenced = in.get();
              if (fenced != 0 && fenced != 1) throw new IOException("Invalid fenced boolean");
              yield new QuorumEntry.BrokerState(epoch, new ClusterRecords.BrokerState(id, brokerEpoch, fenced == 1));
            }
            case 7 -> {
              var id = uuid(in);
              String name = string(in, 249);
              int partitions = in.getInt();
              if (partitions > limits.maxPartitions()) throw new IOException("Partition capacity exceeded");
              yield new QuorumEntry.TopicRecord(epoch, new ClusterRecords.TopicRecord(id, name, partitions));
            }
            case 8 -> {
              var id = uuid(in);
              int partition = in.getInt();
              int count = in.getInt();
              if (count != 1 || in.remaining() < 24 || partition >= limits.maxPartitions())
                throw new IOException("Invalid replica array or partition");
              yield new QuorumEntry.PartitionRecord(epoch, new ClusterRecords.PartitionRecord(
                  id, partition, List.of(in.getInt()), in.getInt(), in.getLong(), in.getLong()));
            }
            default -> throw new IOException("Unknown entry kind");
          };
      if (in.hasRemaining()) throw new IOException("Trailing entry bytes");
      return result;
    } catch (BufferUnderflowException | IllegalArgumentException e) {
      throw new IOException("Invalid entry", e);
    }
  }

  private static boolean isCluster(QuorumEntry entry) {
    return !(entry instanceof QuorumEntry.LeaderChange || entry instanceof QuorumEntry.Topic
        || entry instanceof QuorumEntry.ReadBarrier);
  }

  private static byte clusterKind(QuorumEntry entry) {
    return switch (entry) {
      case QuorumEntry.FeatureLevel ignored -> 4;
      case QuorumEntry.BrokerRegistration ignored -> 5;
      case QuorumEntry.BrokerState ignored -> 6;
      case QuorumEntry.TopicRecord ignored -> 7;
      case QuorumEntry.PartitionRecord ignored -> 8;
      default -> throw new IllegalArgumentException("Not cluster metadata");
    };
  }

  private static void putUuid(ByteBuffer out, UUID id) {
    out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
  }

  private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(), in.getLong()); }

  private static void putString(ByteBuffer out, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.putInt(bytes.length).put(bytes);
  }

  private static String string(ByteBuffer in, int maxBytes) throws IOException {
    int size = in.getInt();
    if (size < 1 || size > maxBytes || size > in.remaining()) throw new IOException("Invalid string length");
    var bytes = in.slice();
    bytes.limit(size);
    in.position(in.position() + size);
    try {
      return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(bytes).toString();
    } catch (CharacterCodingException e) { throw new IOException("Invalid UTF-8", e); }
  }
}

package vn.huyqt.logbroker.controller.log;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
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
    };
  }

  public static byte[] encode(QuorumEntry entry) {
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
    }
    return ByteBuffer.allocate(11 + payload.length)
        .putShort((short) 1)
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
    // No entry can exceed the 1 MiB metadata batch limit.
    if (bytes == null || bytes.length < 11 || bytes.length > 1024 * 1024)
      throw new IOException("Invalid entry length");
    try {
      var in = ByteBuffer.wrap(bytes);
      if (in.getShort() != 1) throw new IOException("Unsupported entry version");
      byte kind = in.get();
      long epoch = in.getLong();
      QuorumEntry result =
          switch (kind) {
            case 1 -> new QuorumEntry.LeaderChange(epoch, in.getInt());
            case 2 -> {
              byte[] payload = new byte[in.remaining()];
              in.get(payload);
              yield new QuorumEntry.Topic(epoch, MetadataEventCodec.decode(payload));
            }
            case 3 -> new QuorumEntry.ReadBarrier(epoch);
            default -> throw new IOException("Unknown entry kind");
          };
      if (in.hasRemaining()) throw new IOException("Trailing entry bytes");
      return result;
    } catch (BufferUnderflowException | IllegalArgumentException e) {
      throw new IOException("Invalid entry", e);
    }
  }
}

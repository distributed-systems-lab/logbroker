package vn.huyqt.logbroker.broker.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Versioned payload in the local metadata partition log.
 *
 * <p>Version 1 encodes a {@link TopicCatalog.TopicCreated} as big-endian {@code int16 version=1},
 * the topic UUID as two {@code int64}, {@code int32 nameLength}, the name bytes and {@code int32
 * partitions}. The payload is stored as the value of a record with timestamp 0, no key and no
 * headers.
 */
public final class MetadataEventCodec {
    private MetadataEventCodec() {}

    /** Encodes {@code event} as a version 1 payload. */
    public static byte[] encode(TopicCatalog.TopicCreated event) {
        byte[] name = event.name().getBytes(StandardCharsets.US_ASCII);
        ByteBuffer target =
                ByteBuffer.allocate(2 + 16 + 4 + name.length + 4).order(ByteOrder.BIG_ENDIAN);
        target.putShort((short) 1)
                .putLong(event.id().getMostSignificantBits())
                .putLong(event.id().getLeastSignificantBits())
                .putInt(name.length)
                .put(name)
                .putInt(event.partitions());
        return target.array();
    }

    /**
     * Strictly decodes a version 1 payload.
     *
     * @throws IOException if the payload is truncated, has an unknown version, trailing bytes, a
     *     name that is not valid UTF-8, or fields that {@link TopicCatalog.TopicCreated} rejects
     */
    public static TopicCatalog.TopicCreated decode(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < 27) throw new IOException("Incomplete metadata event");
        ByteBuffer source = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (source.getShort() != 1) throw new IOException("Unsupported metadata event version");
        UUID id = new UUID(source.getLong(), source.getLong());
        int length = source.getInt();
        if (length < 1 || length > 249 || length > source.remaining() - 4)
            throw new IOException("Invalid metadata topic name length");
        byte[] name = new byte[length];
        source.get(name);
        try {
            String decoded =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(name))
                            .toString();
            int partitions = source.getInt();
            if (source.hasRemaining()) throw new IOException("Trailing metadata bytes");
            return new TopicCatalog.TopicCreated(id, decoded, partitions);
        } catch (CharacterCodingException | IllegalArgumentException error) {
            throw new IOException("Invalid metadata event", error);
        }
    }
}

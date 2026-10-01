package vn.huyqt.logbroker.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;
import vn.huyqt.logbroker.protocol.Protocol.Batch;
import vn.huyqt.logbroker.storage.CorruptLogException;
import vn.huyqt.logbroker.storage.RecordPayloadCodec;

/**
 * Offset-free wire batch; offset is assigned only by the broker.
 *
 * <p>Layout and CRC coverage are specified in {@code docs/protocol-v1.md}; the record payload is
 * {@link RecordPayloadCodec}'s. A wire batch is checked against both the wire and the storage
 * batch limit because the two headers differ in size.
 */
public final class WireBatchCodec {
    private static final int HEADER_BYTES = 14;

    private WireBatchCodec() {
    }

    /** Returns the bytes {@code batch} adds to a Fetch response, including its base offset. */
    public static int fetchSize(Batch batch) {
        return Math.addExact(8 + HEADER_BYTES, RecordPayloadCodec.encodedSize(batch.records()));
    }

    /**
     * Encodes {@code batch} with its CRC32C.
     *
     * @throws ProtocolException {@link ErrorCode#INVALID_REQUEST} for an empty batch, too many
     *     records or an unencodable payload; {@link ErrorCode#BATCH_TOO_LARGE} if the Fetch size
     *     or the equivalent storage batch exceeds its limit
     */
    public static byte[] encode(Batch batch, ProtocolLimits limits) throws ProtocolException {
        Objects.requireNonNull(batch);
        Objects.requireNonNull(limits);
        List<vn.huyqt.logbroker.storage.LogRecord> records = batch.records();
        if (records.isEmpty() || records.size() > limits.maxRecordsPerBatch())
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Invalid record count");
        final int payload;
        try {
            payload = RecordPayloadCodec.encodedSize(records);
        } catch (IllegalArgumentException error) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Invalid record payload", error);
        }
        long fetchSize = (long) payload + HEADER_BYTES + 8;
        if (fetchSize > limits.maxWireBatchBytes()
                || (long) payload + 30 > limits.maxStorageBatchBytes())
            throw new ProtocolException(ErrorCode.BATCH_TOO_LARGE, "Batch exceeds limit");
        byte[] encoded = new byte[payload + HEADER_BYTES];
        ByteBuffer target = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        target.putShort((short) 1).putInt(encoded.length).putInt(records.size()).putInt(0);
        RecordPayloadCodec.write(target, records);
        target.putInt(10, checksum(encoded));
        return encoded;
    }

    /**
     * Decodes exactly one wire batch; {@code bytes} must hold nothing else. Length, record count
     * and limits are checked before the checksum, and the checksum before any record is built.
     *
     * @throws ProtocolException {@link ErrorCode#UNSUPPORTED_VERSION} for an unknown batch
     *     version, {@link ErrorCode#BATCH_TOO_LARGE} for a length outside the limits, otherwise
     *     {@link ErrorCode#INVALID_REQUEST}
     */
    public static Batch decode(byte[] bytes, ProtocolLimits limits) throws ProtocolException {
        Objects.requireNonNull(bytes);
        Objects.requireNonNull(limits);
        // 34 = header plus the smallest record. The same batch costs 8 more bytes in a Fetch
        // response and 16 more as a storage batch (30-byte header instead of 14).
        if (bytes.length < 34 || (long) bytes.length + 8 > limits.maxWireBatchBytes()
                || (long) bytes.length + 16 > limits.maxStorageBatchBytes())
            throw new ProtocolException(ErrorCode.BATCH_TOO_LARGE, "Invalid batch length");
        ByteBuffer source = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (source.getShort() != 1)
            throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, "Unsupported batch version");
        if (source.getInt() != bytes.length)
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Batch length mismatch");
        int count = source.getInt();
        if (count <= 0 || count > limits.maxRecordsPerBatch()
                || count > (bytes.length - HEADER_BYTES) / 20)
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Invalid record count");
        if (source.getInt() != checksum(bytes))
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Batch checksum mismatch");
        try {
            return new Batch(RecordPayloadCodec.read(source, count));
        } catch (CorruptLogException error) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Invalid record payload", error);
        }
    }

    // The CRC field at bytes 10..13 is excluded from its own checksum.
    private static int checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 10);
        crc.update(bytes, 14, bytes.length - 14);
        return (int) crc.getValue();
    }
}

package vn.huyqt.logbroker.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;
import vn.huyqt.logbroker.protocol.Protocol.Batch;
import vn.huyqt.logbroker.storage.CorruptLogException;
import vn.huyqt.logbroker.storage.RecordPayloadCodec;

/** Offset-free wire batch; offset is assigned only by the broker. */
public final class WireBatchCodec {
    private static final int HEADER_BYTES = 14;

    private WireBatchCodec() {
    }

    public static int fetchSize(Batch batch) {
        return Math.addExact(8 + HEADER_BYTES, RecordPayloadCodec.encodedSize(batch.records()));
    }

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

    public static Batch decode(byte[] bytes, ProtocolLimits limits) throws ProtocolException {
        Objects.requireNonNull(bytes);
        Objects.requireNonNull(limits);
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

    private static int checksum(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 10);
        crc.update(bytes, 14, bytes.length - 14);
        return (int) crc.getValue();
    }
}

package vn.huyqt.logbroker.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Encodes and validates version 1 batches using the bounds in {@link LogConfig}. */
final class BatchCodec {
    static final int HEADER_BYTES = 30;
    static final int MIN_BATCH_BYTES = 50;
    private static final byte[] MAGIC = {'D', 'L', 'O', 'G'};

    private BatchCodec() {}

    static byte[] encode(long baseOffset, List<LogRecord> records, int maxBatchBytes) {
        Objects.requireNonNull(records, "records");
        if (maxBatchBytes < MIN_BATCH_BYTES || baseOffset < 0 || records.isEmpty()) {
            throw new IllegalArgumentException("Invalid batch arguments");
        }
        try {
            Math.addExact(baseOffset, records.size());
            long size = Math.addExact(HEADER_BYTES, RecordPayloadCodec.encodedSize(records));
            if (size > maxBatchBytes) throw new IllegalArgumentException("Batch too large");
            byte[] bytes = new byte[(int) size];
            ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            b.put(MAGIC).putShort((short) 1).putInt(bytes.length);
            b.putLong(baseOffset).putInt(records.size()).putInt(0).putInt(0);
            RecordPayloadCodec.write(b, records);
            b.putInt(26, crc(bytes));
            return bytes;
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Batch size or offset overflow", e);
        }
    }

    static RecordBatch decode(byte[] bytes, int maxBatchBytes) throws CorruptLogException {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length < HEADER_BYTES) throw new CorruptLogException("Incomplete batch header");
        validateHeaderPrefix(java.util.Arrays.copyOf(bytes, HEADER_BYTES), maxBatchBytes);
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (b.getInt(6) != bytes.length) throw new CorruptLogException("Batch length mismatch");
        if (b.getInt(26) != crc(bytes)) throw new CorruptLogException("Batch CRC32C mismatch");
        long baseOffset = b.getLong(10);
        int count = b.getInt(18);
        b.position(HEADER_BYTES);
        return new RecordBatch(baseOffset, RecordPayloadCodec.read(b, count), bytes.length);
    }

    static void validateHeaderPrefix(byte[] prefix, int maxBatchBytes) throws CorruptLogException {
        Objects.requireNonNull(prefix, "prefix");
        if (prefix.length > HEADER_BYTES || maxBatchBytes < MIN_BATCH_BYTES) {
            throw new CorruptLogException("Invalid header prefix size");
        }
        for (int i = 0; i < Math.min(4, prefix.length); i++) {
            if (prefix[i] != MAGIC[i]) throw new CorruptLogException("Invalid batch magic");
        }
        ByteBuffer b = ByteBuffer.wrap(prefix).order(ByteOrder.BIG_ENDIAN);
        if (prefix.length >= 6 && b.getShort(4) != 1)
            throw new CorruptLogException("Unsupported batch version");
        int total = 0;
        if (prefix.length >= 10) {
            total = b.getInt(6);
            if (total < MIN_BATCH_BYTES || total > maxBatchBytes) {
                throw new CorruptLogException("Invalid batch length");
            }
        }
        if (prefix.length >= 18 && b.getLong(10) < 0)
            throw new CorruptLogException("Negative offset");
        if (prefix.length >= 22) {
            int count = b.getInt(18);
            if (count <= 0 || count > (total - HEADER_BYTES) / 20) {
                throw new CorruptLogException("Invalid record count");
            }
            try {
                Math.addExact(b.getLong(10), count);
            } catch (ArithmeticException e) {
                throw new CorruptLogException("Offset overflow", e);
            }
        }
        if (prefix.length >= 26 && b.getInt(22) != 0) {
            throw new CorruptLogException("Unsupported batch attributes");
        }
    }

    // The CRC field at bytes 26..29 is excluded from its own checksum.
    private static int crc(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 26);
        crc.update(bytes, 30, bytes.length - 30);
        return (int) crc.getValue();
    }
}

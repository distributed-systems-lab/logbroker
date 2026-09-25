package vn.huyqt.logbroker.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
            long size = HEADER_BYTES;
            for (LogRecord record : records) {
                Objects.requireNonNull(record, "record");
                size = Math.addExact(size, 20);
                size = Math.addExact(size, length(record.key()));
                size = Math.addExact(size, length(record.value()));
                for (RecordHeader header : record.headers()) {
                    size = Math.addExact(size, 8);
                    size = Math.addExact(size, utf8(header.key()).length);
                    size = Math.addExact(size, length(header.value()));
                }
                if (size > maxBatchBytes) throw new IllegalArgumentException("Batch too large");
            }
            if (size > maxBatchBytes) throw new IllegalArgumentException("Batch too large");
            byte[] bytes = new byte[(int) size];
            ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            b.put(MAGIC).putShort((short) 1).putInt(bytes.length);
            b.putLong(baseOffset).putInt(records.size()).putInt(0).putInt(0);
            for (LogRecord record : records) {
                b.putLong(record.timestamp());
                putNullable(b, record.key());
                putNullable(b, record.value());
                b.putInt(record.headers().size());
                for (RecordHeader header : record.headers()) {
                    byte[] key = utf8(header.key());
                    b.putInt(key.length).put(key);
                    putNullable(b, header.value());
                }
            }
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
        List<LogRecord> records = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                if (b.remaining() < 20) throw new CorruptLogException("Incomplete record");
                long timestamp = b.getLong();
                byte[] key = readNullable(b);
                byte[] value = readNullable(b);
                if (b.remaining() < 4) throw new CorruptLogException("Incomplete header count");
                int headerCount = b.getInt();
                if (headerCount < 0 || headerCount > b.remaining() / 8) {
                    throw new CorruptLogException("Invalid header count");
                }
                List<RecordHeader> headers = new ArrayList<>();
                for (int j = 0; j < headerCount; j++) {
                    if (b.remaining() < 4)
                        throw new CorruptLogException("Incomplete header key length");
                    int keyLength = b.getInt();
                    if (keyLength < 0 || keyLength > b.remaining()) {
                        throw new CorruptLogException("Invalid header key length");
                    }
                    byte[] keyBytes = new byte[keyLength];
                    b.get(keyBytes);
                    String headerKey = decodeUtf8(keyBytes);
                    headers.add(new RecordHeader(headerKey, readNullable(b)));
                }
                records.add(new LogRecord(timestamp, key, value, headers));
            }
            if (b.hasRemaining()) throw new CorruptLogException("Trailing batch bytes");
            return new RecordBatch(baseOffset, records, bytes.length);
        } catch (IllegalArgumentException e) {
            throw new CorruptLogException("Invalid batch payload", e);
        }
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

    private static int length(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    private static byte[] utf8(String value) {
        try {
            ByteBuffer b =
                    StandardCharsets.UTF_8
                            .newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .encode(CharBuffer.wrap(value));
            byte[] result = new byte[b.remaining()];
            b.get(result);
            return result;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8 header key", e);
        }
    }

    private static String decodeUtf8(byte[] bytes) throws CorruptLogException {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new CorruptLogException("Malformed UTF-8 header key", e);
        }
    }

    private static void putNullable(ByteBuffer b, byte[] bytes) {
        if (bytes == null) b.putInt(-1);
        else b.putInt(bytes.length).put(bytes);
    }

    private static byte[] readNullable(ByteBuffer b) throws CorruptLogException {
        if (b.remaining() < 4) throw new CorruptLogException("Incomplete nullable length");
        int length = b.getInt();
        if (length == -1) return null;
        if (length < 0 || length > b.remaining())
            throw new CorruptLogException("Invalid nullable length");
        byte[] bytes = new byte[length];
        b.get(bytes);
        return bytes;
    }

    // The CRC field at bytes 26..29 is excluded from its own checksum.
    private static int crc(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 26);
        crc.update(bytes, 30, bytes.length - 30);
        return (int) crc.getValue();
    }
}

package vn.huyqt.logbroker.storage;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Version 1 record payload shared by disk and network batches. */
public final class RecordPayloadCodec {
    private RecordPayloadCodec() {}

    public static int encodedSize(List<LogRecord> records) {
        Objects.requireNonNull(records, "records");
        try {
            long size = 0;
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
            }
            return Math.toIntExact(size);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Record payload size overflow", e);
        }
    }

    public static void write(ByteBuffer target, List<LogRecord> records) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(records, "records");
        for (LogRecord record : records) {
            target.putLong(record.timestamp());
            putNullable(target, record.key());
            putNullable(target, record.value());
            target.putInt(record.headers().size());
            for (RecordHeader header : record.headers()) {
                byte[] key = utf8(header.key());
                target.putInt(key.length).put(key);
                putNullable(target, header.value());
            }
        }
    }

    /** Consumes a bounded payload containing exactly {@code count} records. */
    public static List<LogRecord> read(ByteBuffer payload, int count) throws CorruptLogException {
        Objects.requireNonNull(payload, "payload");
        if (count < 0 || count > payload.remaining() / 20) {
            throw new CorruptLogException("Invalid record count");
        }
        List<LogRecord> records = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                if (payload.remaining() < 20) throw new CorruptLogException("Incomplete record");
                long timestamp = payload.getLong();
                byte[] key = readNullable(payload);
                byte[] value = readNullable(payload);
                if (payload.remaining() < 4)
                    throw new CorruptLogException("Incomplete header count");
                int headerCount = payload.getInt();
                if (headerCount < 0 || headerCount > payload.remaining() / 8)
                    throw new CorruptLogException("Invalid header count");
                List<RecordHeader> headers = new ArrayList<>(headerCount);
                for (int j = 0; j < headerCount; j++) {
                    if (payload.remaining() < 4)
                        throw new CorruptLogException("Incomplete header key length");
                    int keyLength = payload.getInt();
                    if (keyLength < 0 || keyLength > payload.remaining())
                        throw new CorruptLogException("Invalid header key length");
                    byte[] keyBytes = new byte[keyLength];
                    payload.get(keyBytes);
                    headers.add(new RecordHeader(decodeUtf8(keyBytes), readNullable(payload)));
                }
                records.add(new LogRecord(timestamp, key, value, headers));
            }
            if (payload.hasRemaining()) throw new CorruptLogException("Trailing batch bytes");
            return List.copyOf(records);
        } catch (IllegalArgumentException e) {
            throw new CorruptLogException("Invalid batch payload", e);
        }
    }

    private static int length(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    private static byte[] utf8(String value) {
        try {
            ByteBuffer bytes = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] result = new byte[bytes.remaining()];
            bytes.get(result);
            return result;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid UTF-8 header key", e);
        }
    }

    private static String decodeUtf8(byte[] bytes) throws CorruptLogException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new CorruptLogException("Malformed UTF-8 header key", e);
        }
    }

    private static void putNullable(ByteBuffer target, byte[] bytes) {
        if (bytes == null) target.putInt(-1);
        else target.putInt(bytes.length).put(bytes);
    }

    private static byte[] readNullable(ByteBuffer source) throws CorruptLogException {
        if (source.remaining() < 4) throw new CorruptLogException("Incomplete nullable length");
        int length = source.getInt();
        if (length == -1) return null;
        if (length < 0 || length > source.remaining())
            throw new CorruptLogException("Invalid nullable length");
        byte[] bytes = new byte[length];
        source.get(bytes);
        return bytes;
    }
}

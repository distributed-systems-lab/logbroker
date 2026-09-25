package vn.huyqt.logbroker.protocol;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import vn.huyqt.logbroker.protocol.Protocol.*;
import vn.huyqt.logbroker.protocol.Protocol.Error;

/** Bounded version 1 request/response codec independent of Netty. */
public final class ProtocolCodec {
    private static final short VERSION = 1;
    private static final int MAX_STRING = 249;
    private final ProtocolLimits limits;

    public ProtocolCodec(ProtocolLimits limits) {
        this.limits = Objects.requireNonNull(limits);
    }

    public byte[] encodeRequest(RequestFrame frame) throws ProtocolException {
        requireHeader(frame.operation(), frame.version(), frame.requestId());
        try {
            return encode(frame.operation(), frame.version(), frame.requestId(), out -> {
                switch (frame.body()) {
                    case CreateTopic request -> {
                        requireOperation(frame.operation(), 1);
                        putString(out, request.name(), MAX_STRING);
                        out.writeInt(request.partitions());
                    }
                    case Metadata request -> {
                        requireOperation(frame.operation(), 2);
                        putArrayCount(out, request.names().size(), 128);
                        for (String name : request.names()) putString(out, name, MAX_STRING);
                    }
                    case Produce request -> {
                        requireOperation(frame.operation(), 3);
                        out.writeByte(request.ack() == AckMode.APPENDED ? 0 : 1);
                        out.writeInt(request.timeoutMs());
                        putArrayCount(out, request.entries().size(), limits.maxPartitionEntries());
                        for (ProduceEntry entry : request.entries()) {
                            putPartition(out, entry.partition());
                            out.write(WireBatchCodec.encode(entry.batch(), limits));
                        }
                    }
                    case Fetch request -> {
                        requireOperation(frame.operation(), 4);
                        out.writeInt(request.maxBytes());
                        out.writeInt(request.minBytes());
                        out.writeInt(request.maxWaitMs());
                        putArrayCount(out, request.entries().size(), limits.maxPartitionEntries());
                        for (FetchEntry entry : request.entries()) {
                            putPartition(out, entry.partition());
                            out.writeLong(entry.offset());
                            out.writeInt(entry.maxBytes());
                        }
                    }
                }
            });
        } catch (IOException error) {
            throw asProtocol(error);
        }
    }

    public RequestFrame decodeRequest(byte[] frame) throws ProtocolException {
        ByteBuffer source = header(frame);
        short operation = source.getShort();
        short version = source.getShort();
        long requestId = source.getLong();
        requireHeader(operation, version, requestId);
        Request body = switch (operation) {
            case 1 -> new CreateTopic(getString(source, MAX_STRING), getInt(source));
            case 2 -> {
                int count = getCount(source, 128, 4);
                List<String> names = new ArrayList<>(count);
                Set<String> unique = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    String name = getString(source, MAX_STRING);
                    if (!unique.add(name)) throw invalid("Duplicate topic name");
                    names.add(name);
                }
                yield new Metadata(names);
            }
            case 3 -> {
                int ack = Byte.toUnsignedInt(getByte(source));
                if (ack > 1) throw invalid("Invalid acknowledgment mode");
                int timeout = getInt(source);
                if (timeout <= 0 || timeout > 30_000) throw invalid("Invalid Produce timeout");
                int count = getCount(source, limits.maxPartitionEntries(), 16 + 4 + 34);
                if (count == 0) throw invalid("Empty Produce");
                List<ProduceEntry> entries = new ArrayList<>(count);
                Set<TopicPartition> unique = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    TopicPartition tp = getPartition(source);
                    if (!unique.add(tp)) throw invalid("Duplicate partition");
                    entries.add(new ProduceEntry(tp, WireBatchCodec.decode(getWireBatch(source), limits)));
                }
                yield new Produce(ack == 0 ? AckMode.APPENDED : AckMode.FLUSHED, timeout, entries);
            }
            case 4 -> {
                int max = getInt(source), min = getInt(source), wait = getInt(source);
                if (max <= 0 || max > 4 * 1024 * 1024 || min < 0 || min > max
                        || wait < 0 || wait > 5_000) throw invalid("Invalid Fetch limits");
                int count = getCount(source, limits.maxPartitionEntries(), 16 + 4 + 8 + 4);
                if (count == 0) throw invalid("Empty Fetch");
                List<FetchEntry> entries = new ArrayList<>(count);
                Set<TopicPartition> unique = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    TopicPartition tp = getPartition(source);
                    if (!unique.add(tp)) throw invalid("Duplicate partition");
                    long offset = getLong(source);
                    int partitionMax = getInt(source);
                    if (offset < 0 || partitionMax <= 0) throw invalid("Invalid Fetch entry");
                    entries.add(new FetchEntry(tp, offset, partitionMax));
                }
                yield new Fetch(max, min, wait, entries);
            }
            default -> throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
        };
        requireConsumed(source);
        return new RequestFrame(operation, version, requestId, body);
    }

    public byte[] encodeResponse(ResponseFrame frame) throws ProtocolException {
        if (!(frame.body() instanceof Failure)) {
            requireHeader(frame.operation(), frame.version(), frame.requestId());
        }
        try {
            return encode(frame.operation(), frame.version(), frame.requestId(), out -> {
                Response body = frame.body();
                Error requestError = switch (body) {
                    case CreateTopicReply reply -> reply.error();
                    case MetadataReply reply -> reply.error();
                    case ProduceReply reply -> reply.error();
                    case FetchReply reply -> reply.error();
                    case Failure failure -> failure.error();
                };
                putError(out, requestError);
                if (requestError.code() != ErrorCode.NONE) return;
                switch (body) {
                    case CreateTopicReply reply -> { requireOperation(frame.operation(), 1); putUuid(out, reply.topicId()); }
                    case MetadataReply reply -> {
                        requireOperation(frame.operation(), 2);
                        putString(out, reply.host(), 255);
                        out.writeInt(reply.port());
                        putArrayCount(out, reply.topics().size(), 128);
                        for (TopicInfo topic : reply.topics()) {
                            putString(out, topic.name(), MAX_STRING);
                            putUuid(out, topic.id());
                            putArrayCount(out, topic.partitions().size(), 1024);
                            for (PartitionInfo partition : topic.partitions()) {
                                out.writeInt(partition.partition());
                                putError(out, partition.error());
                            }
                        }
                    }
                    case ProduceReply reply -> {
                        requireOperation(frame.operation(), 3);
                        putArrayCount(out, reply.results().size(), limits.maxPartitionEntries());
                        for (ProduceResult result : reply.results()) {
                            putPartition(out, result.partition());
                            putError(out, result.error());
                            out.writeLong(result.firstOffset());
                            out.writeLong(result.nextOffset());
                        }
                    }
                    case FetchReply reply -> {
                        requireOperation(frame.operation(), 4);
                        putArrayCount(out, reply.results().size(), limits.maxPartitionEntries());
                        for (FetchResult result : reply.results()) {
                            putPartition(out, result.partition());
                            putError(out, result.error());
                            out.writeLong(result.logStartOffset());
                            out.writeLong(result.logEndOffset());
                            putArrayCount(out, result.batches().size(), limits.maxFrameBytes() / 42);
                            for (FetchBatch batch : result.batches()) {
                                out.writeLong(batch.baseOffset());
                                out.write(WireBatchCodec.encode(batch.batch(), limits));
                            }
                        }
                    }
                    case Failure ignored -> throw invalid("NONE is not a failure");
                }
            });
        } catch (IOException error) {
            throw asProtocol(error);
        }
    }

    public ResponseFrame decodeResponse(byte[] frame) throws ProtocolException {
        ByteBuffer source = header(frame);
        short operation = source.getShort();
        short version = source.getShort();
        long requestId = source.getLong();
        if (requestId < 0) throw invalid("Invalid response header");
        Error error = getError(source);
        if (error.code() != ErrorCode.NONE) {
            requireConsumed(source);
            return new ResponseFrame(operation, version, requestId, new Failure(error));
        }
        requireHeader(operation, version, requestId);
        Response body = switch (operation) {
            case 1 -> new CreateTopicReply(error, getUuid(source));
            case 2 -> {
                String host = getString(source, 255);
                int port = getInt(source);
                int count = getCount(source, 128, 16);
                List<TopicInfo> topics = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String name = getString(source, MAX_STRING);
                    UUID id = getUuid(source);
                    int partitions = getCount(source, 1024, 10);
                    List<PartitionInfo> infos = new ArrayList<>(partitions);
                    for (int j = 0; j < partitions; j++) {
                        infos.add(new PartitionInfo(getInt(source), getError(source)));
                    }
                    topics.add(new TopicInfo(name, id, infos));
                }
                yield new MetadataReply(error, host, port, topics);
            }
            case 3 -> {
                int count = getCount(source, limits.maxPartitionEntries(), 16 + 4 + 2 + 4 + 16);
                List<ProduceResult> results = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    results.add(new ProduceResult(getPartition(source), getError(source),
                            getLong(source), getLong(source)));
                }
                yield new ProduceReply(error, results);
            }
            case 4 -> {
                int count = getCount(source, limits.maxPartitionEntries(), 16 + 4 + 2 + 4 + 16 + 4);
                List<FetchResult> results = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    TopicPartition partition = getPartition(source);
                    Error entryError = getError(source);
                    long start = getLong(source), end = getLong(source);
                    int batchCount = getCount(source, limits.maxFrameBytes() / 42, 8 + 34);
                    List<FetchBatch> batches = new ArrayList<>(batchCount);
                    for (int j = 0; j < batchCount; j++) {
                        batches.add(new FetchBatch(getLong(source),
                                WireBatchCodec.decode(getWireBatch(source), limits)));
                    }
                    results.add(new FetchResult(partition, entryError, start, end, batches));
                }
                yield new FetchReply(error, results);
            }
            default -> throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
        };
        requireConsumed(source);
        return new ResponseFrame(operation, version, requestId, body);
    }

    private byte[] encode(short operation, short version, long requestId, Writer writer)
            throws IOException {
        if (requestId < 0) throw invalid("Negative request ID");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(0);
        out.writeShort(operation);
        out.writeShort(version);
        out.writeLong(requestId);
        writer.write(out);
        byte[] frame = bytes.toByteArray();
        if (frame.length - 4 > limits.maxFrameBytes())
            throw new ProtocolException(ErrorCode.BATCH_TOO_LARGE, "Frame too large");
        ByteBuffer.wrap(frame).putInt(frame.length - 4);
        return frame;
    }

    private ByteBuffer header(byte[] frame) throws ProtocolException {
        Objects.requireNonNull(frame);
        if (frame.length < 16 || frame.length - 4 > limits.maxFrameBytes())
            throw invalid("Frame length out of range");
        ByteBuffer source = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN);
        if (source.getInt() != frame.length - 4) throw invalid("Frame length mismatch");
        return source;
    }

    private static void requireHeader(short operation, short version, long id)
            throws ProtocolException {
        if (version != VERSION) throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, "Unsupported version");
        if (id < 0) throw invalid("Negative request ID");
        if (operation < 1 || operation > 4)
            throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
    }

    private static void requireOperation(short actual, int expected) throws ProtocolException {
        if (actual != expected) throw invalid("Operation/body mismatch");
    }

    private static void requireConsumed(ByteBuffer source) throws ProtocolException {
        if (source.hasRemaining()) throw invalid("Trailing frame bytes");
    }

    private static void putArrayCount(DataOutputStream out, int count, int max)
            throws IOException {
        if (count < 0 || count > max) throw invalid("Array count exceeds limit");
        out.writeInt(count);
    }

    private static void putString(DataOutputStream out, String value, int max)
            throws IOException {
        Objects.requireNonNull(value);
        byte[] encoded = strictUtf8(value);
        if (encoded.length > max) throw invalid("String exceeds limit");
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static byte[] strictUtf8(String value) throws ProtocolException {
        try {
            ByteBuffer buffer = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(java.nio.CharBuffer.wrap(value));
            byte[] result = new byte[buffer.remaining()];
            buffer.get(result);
            return result;
        } catch (CharacterCodingException error) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Malformed string", error);
        }
    }

    private static String getString(ByteBuffer source, int max) throws ProtocolException {
        int size = getInt(source);
        if (size < 0 || size > max || size > source.remaining()) throw invalid("Invalid string length");
        ByteBuffer slice = source.slice();
        slice.limit(size);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(slice).toString();
            source.position(source.position() + size);
            return value;
        } catch (CharacterCodingException error) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Malformed UTF-8", error);
        }
    }

    private static void putUuid(DataOutputStream out, UUID id) throws IOException {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    private static UUID getUuid(ByteBuffer source) throws ProtocolException {
        return new UUID(getLong(source), getLong(source));
    }

    private static void putPartition(DataOutputStream out, TopicPartition partition)
            throws IOException {
        putUuid(out, partition.topicId());
        out.writeInt(partition.partition());
    }

    private static TopicPartition getPartition(ByteBuffer source) throws ProtocolException {
        UUID id = getUuid(source);
        int partition = getInt(source);
        if (partition < 0) throw invalid("Negative partition");
        return new TopicPartition(id, partition);
    }

    private static void putError(DataOutputStream out, Error error) throws IOException {
        out.writeShort(error.code().number());
        putString(out, error.message(), 512);
    }

    private static Error getError(ByteBuffer source) throws ProtocolException {
        return new Error(ErrorCode.fromNumber(getShort(source)), getString(source, 512));
    }

    private byte[] getWireBatch(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 14) throw invalid("Incomplete wire batch header");
        int length = source.getInt(source.position() + 2);
        if (length < 34 || length > source.remaining()) throw invalid("Invalid wire batch length");
        if ((long) length + 8 > limits.maxWireBatchBytes()
                || (long) length + 16 > limits.maxStorageBatchBytes())
            throw new ProtocolException(ErrorCode.BATCH_TOO_LARGE, "Wire batch exceeds limit");
        byte[] bytes = new byte[length];
        source.get(bytes);
        return bytes;
    }

    private static int getCount(ByteBuffer source, int max, int minBytes) throws ProtocolException {
        int count = getInt(source);
        if (count < 0 || count > max || count > source.remaining() / minBytes)
            throw invalid("Invalid array count");
        return count;
    }

    private static byte getByte(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 1) throw invalid("Incomplete byte");
        return source.get();
    }

    private static short getShort(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 2) throw invalid("Incomplete short");
        return source.getShort();
    }

    private static int getInt(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 4) throw invalid("Incomplete integer");
        return source.getInt();
    }

    private static long getLong(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 8) throw invalid("Incomplete long");
        return source.getLong();
    }

    private static ProtocolException invalid(String reason) {
        return new ProtocolException(ErrorCode.INVALID_REQUEST, reason);
    }

    private static ProtocolException asProtocol(IOException error) {
        return error instanceof ProtocolException known
                ? known : new ProtocolException(ErrorCode.INVALID_REQUEST, "Codec I/O error", error);
    }

    @FunctionalInterface
    private interface Writer { void write(DataOutputStream output) throws IOException; }
}

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

/**
 * Bounded version 1 and 2 request/response codec independent of Netty.
 *
 * <p>Frames are complete byte arrays including the leading {@code int32 frameLength}; the layout
 * is specified in {@code docs/protocol-v1.md}. Every count and length is checked against the
 * remaining bytes and the configured {@link ProtocolLimits} before allocation, and a decoded body
 * must consume the frame exactly.
 *
 * <p>Instances hold only immutable limits and may be shared between threads.
 */
public final class ProtocolCodec {
    private static final short VERSION = 1;
    // Topic names are at most 249 ASCII characters; see section 4 of
    // docs/superpowers/specs/2026-09-25-broker-phase-2-design.md.
    private static final int MAX_STRING = 249;
    private final ProtocolLimits limits;
    private final vn.huyqt.logbroker.controller.metadata.MetadataLimits clusterLimits;

    public ProtocolCodec(ProtocolLimits limits) {
        this(limits, vn.huyqt.logbroker.controller.metadata.MetadataLimits.defaults());
    }

    public ProtocolCodec(ProtocolLimits limits, vn.huyqt.logbroker.controller.metadata.MetadataLimits clusterLimits) {
        this.limits = Objects.requireNonNull(limits);
        this.clusterLimits = Objects.requireNonNull(clusterLimits);
    }

    /**
     * Conservative decoded-memory admission before record/header allocation.
     *
     * <p>Returns twice the frame length, plus a per-record and per-header allowance for a
     * well-formed Produce request. For a malformed non-null frame it returns the base estimate
     * instead of throwing and leaves the error to {@link #decodeRequest}.
     */
    public long estimatedDecodedBytes(byte[] frame) {
        long base = 2L * frame.length;
        try {
            ByteBuffer source = header(frame);
            short operation = source.getShort();
            short version = source.getShort();
            source.getLong();
            if (operation != 3)
                return base;
            if (version == 2) getUuid(source);
            getByte(source);
            getInt(source);
            int partitions = getCount(source, limits.maxPartitionEntries(), 54);
            long records = 0, headers = 0;
            for (int partition = 0; partition < partitions; partition++) {
                if (source.remaining() < 34)
                    return base;
                source.position(source.position() + (version == 2 ? 40 : 20));
                int start = source.position();
                int length = source.getInt(start + 2);
                int count = source.getInt(start + 6);
                if (length < 34 || length > source.remaining()
                        || count < 1 || count > limits.maxRecordsPerBatch())
                    return base;
                var payload = source.duplicate();
                payload.position(start + 14).limit(start + length);
                for (int record = 0; record < count; record++) {
                    if (payload.remaining() < 20)
                        return base;
                    payload.position(payload.position() + 8);
                    if (!skipNullable(payload) || !skipNullable(payload))
                        return base;
                    int headerCount = getInt(payload);
                    if (headerCount < 0 || headerCount > payload.remaining() / 8)
                        return base;
                    records++;
                    headers += headerCount;
                    for (int header = 0; header < headerCount; header++) {
                        int keyLength = getInt(payload);
                        if (keyLength < 0 || keyLength > payload.remaining())
                            return base;
                        payload.position(payload.position() + keyLength);
                        if (!skipNullable(payload))
                            return base;
                    }
                }
                if (payload.hasRemaining())
                    return base;
                source.position(start + length);
            }
            return source.hasRemaining() ? base : base + 64L * records + 64L * headers;
        } catch (ProtocolException | RuntimeException malformed) {
            // Decoder will produce the protocol error without constructing records.
            return base;
        }
    }

    private static boolean skipNullable(ByteBuffer source) throws ProtocolException {
        int length = getInt(source);
        if (length == -1)
            return true;
        if (length < 0 || length > source.remaining())
            return false;
        source.position(source.position() + length);
        return true;
    }

    /**
     * Encodes a complete request frame.
     *
     * @throws ProtocolException if the header is invalid, the operation does not match the body
     *     type, a string, array or batch exceeds its limit, or the frame exceeds {@link
     *     ProtocolLimits#maxFrameBytes()}
     */
    public byte[] encodeRequest(RequestFrame frame) throws ProtocolException {
        requireHeader(frame.operation(), frame.version(), frame.requestId());
        if (frame.version() == 2) return encodeClusterRequest(frame);
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
                        // Equals the default catalog cap of 128 topics.
                        putArrayCount(out, request.names().size(), 128);
                        for (String name : request.names())
                            putString(out, name, MAX_STRING);
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
                    default -> throw invalid("Version/body mismatch");
                }
            });
        } catch (IOException error) {
            throw asProtocol(error);
        }
    }

    /**
     * Decodes and validates a complete request frame, including semantic bounds such as the
     * Produce timeout, Fetch limits and duplicate partitions.
     *
     * @throws ProtocolException carrying the code to report: {@link ErrorCode#UNSUPPORTED_VERSION}
     *     or {@link ErrorCode#UNSUPPORTED_OPERATION} for a valid envelope with an unknown version
     *     or operation, {@link ErrorCode#BATCH_TOO_LARGE} for an oversized batch, otherwise
     *     {@link ErrorCode#INVALID_REQUEST}
     */
    public RequestFrame decodeRequest(byte[] frame) throws ProtocolException {
        ByteBuffer source = header(frame);
        short operation = source.getShort();
        short version = source.getShort();
        long requestId = source.getLong();
        requireHeader(operation, version, requestId);
        if (version == 2) return decodeClusterRequest(source, operation, requestId);
        Request body = switch (operation) {
            case 1 -> new CreateTopic(getString(source, MAX_STRING), getInt(source));
            case 2 -> {
                int count = getCount(source, 128, 4);
                List<String> names = new ArrayList<>(count);
                Set<String> unique = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    String name = getString(source, MAX_STRING);
                    if (!unique.add(name))
                        throw invalid("Duplicate topic name");
                    names.add(name);
                }
                yield new Metadata(names);
            }
            case 3 -> {
                int ack = Byte.toUnsignedInt(getByte(source));
                if (ack > 1)
                    throw invalid("Invalid acknowledgment mode");
                int timeout = getInt(source);
                // Equals the maximum broker processing timeout (30 s) in the Phase 2 design.
                if (timeout <= 0 || timeout > 30_000)
                    throw invalid("Invalid Produce timeout");
                int count = getCount(source, limits.maxPartitionEntries(), 16 + 4 + 34);
                if (count == 0)
                    throw invalid("Empty Produce");
                List<ProduceEntry> entries = new ArrayList<>(count);
                Set<TopicPartition> unique = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    TopicPartition tp = getPartition(source);
                    if (!unique.add(tp))
                        throw invalid("Duplicate partition");
                    entries.add(new ProduceEntry(tp, WireBatchCodec.decode(getWireBatch(source), limits)));
                }
                yield new Produce(ack == 0 ? AckMode.APPENDED : AckMode.FLUSHED, timeout, entries);
            }
            case 4 -> {
                int max = getInt(source), min = getInt(source), wait = getInt(source);
                // Phase 2 design caps: Fetch batch budget 4 MiB, maxWaitMs 5 s.
                if (max <= 0 || max > 4 * 1024 * 1024 || min < 0 || min > max
                        || wait < 0 || wait > 5_000)
                    throw invalid("Invalid Fetch limits");
                int count = getCount(source, limits.maxPartitionEntries(), 16 + 4 + 8 + 4);
                if (count == 0)
                    throw invalid("Empty Fetch");
                List<FetchEntry> entries = new ArrayList<>(count);
                Set<TopicPartition> unique = new HashSet<>();
                for (int i = 0; i < count; i++) {
                    TopicPartition tp = getPartition(source);
                    if (!unique.add(tp))
                        throw invalid("Duplicate partition");
                    long offset = getLong(source);
                    int partitionMax = getInt(source);
                    if (offset < 0 || partitionMax <= 0)
                        throw invalid("Invalid Fetch entry");
                    entries.add(new FetchEntry(tp, offset, partitionMax));
                }
                yield new Fetch(max, min, wait, entries);
            }
            default -> throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
        };
        requireConsumed(source);
        return new RequestFrame(operation, version, requestId, body);
    }

    /**
     * Encodes a complete response frame. If the top-level error is not {@code NONE}, only the
     * error is written, whatever the body type.
     *
     * @throws ProtocolException if the header or body violates the codec's limits, or a {@link
     *     Failure} carries {@code NONE}
     */
    public byte[] encodeResponse(ResponseFrame frame) throws ProtocolException {
        if (frame.version() == 2 && !(frame.body() instanceof Failure)) return encodeClusterResponse(frame);
        // Failures may answer an unknown operation or version, which is echoed unchanged.
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
                    default -> throw invalid("Version/body mismatch");
                };
                putError(out, requestError);
                if (requestError.code() != ErrorCode.NONE)
                    return;
                switch (body) {
                    case CreateTopicReply reply -> {
                        requireOperation(frame.operation(), 1);
                        putUuid(out, reply.topicId());
                    }
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
                            // 42 bytes is the smallest Fetch batch: base offset plus 34-byte batch.
                            putArrayCount(out, result.batches().size(), limits.maxFrameBytes() / 42);
                            for (FetchBatch batch : result.batches()) {
                                out.writeLong(batch.baseOffset());
                                out.write(WireBatchCodec.encode(batch.batch(), limits));
                            }
                        }
                    }
                    case Failure ignored -> throw invalid("NONE is not a failure");
                    default -> throw invalid("Version/body mismatch");
                }
            });
        } catch (IOException error) {
            throw asProtocol(error);
        }
    }

    /**
     * Decodes a complete response frame. A nonzero top-level error decodes as {@link Failure}.
     *
     * @throws ProtocolException if the frame is malformed or exceeds the limits
     */
    public ResponseFrame decodeResponse(byte[] frame) throws ProtocolException {
        ByteBuffer source = header(frame);
        short operation = source.getShort();
        short version = source.getShort();
        long requestId = source.getLong();
        if (requestId < 0)
            throw invalid("Invalid response header");
        // Operation and version are checked only for success bodies: an error reply may echo an
        // unsupported operation or version.
        Error error = getError(source);
        if (error.code() != ErrorCode.NONE) {
            requireConsumed(source);
            return new ResponseFrame(operation, version, requestId, new Failure(error));
        }
        requireHeader(operation, version, requestId);
        if (version == 2) return decodeClusterResponse(source, operation, requestId, error);
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

    private byte[] encodeClusterRequest(RequestFrame frame) throws ProtocolException {
        try {
            byte[] bytes = encode(frame.operation(), frame.version(), frame.requestId(), out -> {
                switch (frame.body()) {
                    case ClusterProtocol.CreateTopic request -> {
                        requireOperation(frame.operation(), 1); putUuid(out, request.clusterId());
                        putString(out, request.name(), MAX_STRING); out.writeInt(request.partitions());
                        out.writeShort(request.replicationFactor()); out.writeInt(request.timeoutMs());
                    }
                    case ClusterProtocol.Metadata request -> {
                        requireOperation(frame.operation(), 2); putUuid(out, request.clusterId());
                        putArrayCount(out, request.names().size(), clusterLimits.maxTopics());
                        for (String name : request.names()) putString(out, name, MAX_STRING);
                    }
                    case ClusterProtocol.Produce request -> {
                        requireOperation(frame.operation(), 3); putUuid(out, request.clusterId());
                        out.writeByte(request.ack().ordinal()); out.writeInt(request.timeoutMs());
                        putArrayCount(out, request.entries().size(), limits.maxPartitionEntries());
                        for (var entry : request.entries()) {
                            putRoute(out, entry.route()); out.write(WireBatchCodec.encode(entry.batch(), limits));
                        }
                    }
                    case ClusterProtocol.Fetch request -> {
                        requireOperation(frame.operation(), 4); putUuid(out, request.clusterId());
                        out.writeInt(request.maxBytes()); out.writeInt(request.minBytes()); out.writeInt(request.maxWaitMs());
                        putArrayCount(out, request.entries().size(), limits.maxPartitionEntries());
                        for (var entry : request.entries()) {
                            putRoute(out, entry.route()); out.writeLong(entry.offset()); out.writeInt(entry.maxBytes());
                        }
                        out.writeBoolean(request.allowOversizedFirstBatch());
                    }
                    default -> throw invalid("Version/body mismatch");
                }
            });
            decodeRequest(bytes); // Apply the same semantic/duplicate validation on both boundaries.
            return bytes;
        } catch (IOException error) { throw asProtocol(error); }
    }

    private RequestFrame decodeClusterRequest(ByteBuffer source, short operation, long id) throws ProtocolException {
        try {
            UUID cluster = getUuid(source);
            Request body = switch (operation) {
                case 1 -> new ClusterProtocol.CreateTopic(cluster, getString(source, MAX_STRING), getInt(source),
                        getShort(source), getInt(source));
                case 2 -> {
                    int count = getCount(source, clusterLimits.maxTopics(), 4);
                    List<String> names = new ArrayList<>(count); Set<String> unique = new HashSet<>();
                    for (int i = 0; i < count; i++) {
                        String name = getString(source, MAX_STRING);
                        if (!unique.add(name)) throw invalid("Duplicate topic name"); names.add(name);
                    }
                    yield new ClusterProtocol.Metadata(cluster, names);
                }
                case 3 -> {
                    int ack = Byte.toUnsignedInt(getByte(source)); if (ack > 1) throw invalid("Invalid ack");
                    int timeout = getInt(source), count = getCount(source, limits.maxPartitionEntries(), 74);
                    List<ClusterProtocol.ProduceEntry> entries = new ArrayList<>(count); Set<TopicPartition> unique = new HashSet<>();
                    for (int i = 0; i < count; i++) {
                        var route = getRoute(source); if (!unique.add(route.partition())) throw invalid("Duplicate partition");
                        entries.add(new ClusterProtocol.ProduceEntry(route, WireBatchCodec.decode(getWireBatch(source), limits)));
                    }
                    yield new ClusterProtocol.Produce(cluster, AckMode.values()[ack], timeout, entries);
                }
                case 4 -> {
                    int max = getInt(source), min = getInt(source), wait = getInt(source);
                    int count = getCount(source, limits.maxPartitionEntries(), 52);
                    List<ClusterProtocol.FetchEntry> entries = new ArrayList<>(count); Set<TopicPartition> unique = new HashSet<>();
                    for (int i = 0; i < count; i++) {
                        var route = getRoute(source); if (!unique.add(route.partition())) throw invalid("Duplicate partition");
                        entries.add(new ClusterProtocol.FetchEntry(route, getLong(source), getInt(source)));
                    }
                    yield new ClusterProtocol.Fetch(cluster, max, min, wait, entries, getBoolean(source));
                }
                default -> throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
            };
            requireConsumed(source); return new RequestFrame(operation, (short) 2, id, body);
        } catch (IllegalArgumentException | ArithmeticException error) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Invalid cluster request", error);
        }
    }

    private byte[] encodeClusterResponse(ResponseFrame frame) throws ProtocolException {
        requireHeader(frame.operation(), frame.version(), frame.requestId());
        try {
            return encode(frame.operation(), frame.version(), frame.requestId(), out -> {
                Error error = switch (frame.body()) {
                    case ClusterProtocol.CreateTopicReply reply -> reply.error();
                    case ClusterProtocol.MetadataReply reply -> reply.error();
                    case ClusterProtocol.ProduceReply reply -> reply.error();
                    case ClusterProtocol.FetchReply reply -> reply.error();
                    default -> throw invalid("Version/body mismatch");
                };
                putError(out, error); if (error.code() != ErrorCode.NONE) return;
                switch (frame.body()) {
                    case ClusterProtocol.CreateTopicReply reply -> {
                        requireOperation(frame.operation(), 1); putUuid(out, reply.topicId()); out.writeLong(reply.commitOffset());
                    }
                    case ClusterProtocol.MetadataReply reply -> {
                        requireOperation(frame.operation(), 2); putUuid(out, reply.clusterId()); out.writeLong(reply.appliedOffset());
                        putArrayCount(out, reply.brokers().size(), clusterLimits.maxBrokers());
                        for (var broker : reply.brokers()) {
                            out.writeInt(broker.id()); putString(out, broker.endpoint().host(), 255);
                            out.writeInt(broker.endpoint().port()); out.writeLong(broker.brokerEpoch()); out.writeBoolean(broker.fenced());
                        }
                        putArrayCount(out, reply.topics().size(), clusterLimits.maxTopics());
                        for (var topic : reply.topics()) {
                            putString(out, topic.name(), MAX_STRING); putUuid(out, topic.id());
                            putArrayCount(out, topic.partitions().size(), clusterLimits.maxPartitions());
                            for (var partition : topic.partitions()) {
                                out.writeInt(partition.partition()); putError(out, partition.error());
                                putArrayCount(out, partition.replicas().size(), 1);
                                for (int replica : partition.replicas()) out.writeInt(replica);
                                out.writeInt(partition.leaderId()); out.writeLong(partition.leaderEpoch()); out.writeLong(partition.partitionEpoch());
                            }
                        }
                    }
                    case ClusterProtocol.ProduceReply reply -> {
                        requireOperation(frame.operation(), 3); putArrayCount(out, reply.results().size(), limits.maxPartitionEntries());
                        for (var result : reply.results()) {
                            putPartition(out, result.partition()); putError(out, result.error()); out.writeByte(result.outcome().ordinal());
                            out.writeLong(result.firstOffset()); out.writeLong(result.nextOffset());
                        }
                    }
                    case ClusterProtocol.FetchReply reply -> {
                        requireOperation(frame.operation(), 4); putArrayCount(out, reply.results().size(), limits.maxPartitionEntries());
                        for (var result : reply.results()) {
                            putPartition(out, result.partition()); putError(out, result.error());
                            out.writeLong(result.logStartOffset()); out.writeLong(result.logEndOffset()); out.writeLong(result.highWatermark());
                            putArrayCount(out, result.batches().size(), limits.maxFrameBytes() / 42);
                            for (var batch : result.batches()) {
                                out.writeLong(batch.baseOffset()); out.write(WireBatchCodec.encode(batch.batch(), limits));
                            }
                        }
                    }
                    default -> throw invalid("Version/body mismatch");
                }
            });
        } catch (IOException error) { throw asProtocol(error); }
    }

    private ResponseFrame decodeClusterResponse(ByteBuffer source, short operation, long id, Error error) throws ProtocolException {
        try {
            Response body = switch (operation) {
                case 1 -> new ClusterProtocol.CreateTopicReply(error, getUuid(source), getLong(source));
                case 2 -> {
                    UUID cluster = getUuid(source); long offset = getLong(source);
                    int count = getCount(source, clusterLimits.maxBrokers(), 21); var brokers = new ArrayList<ClusterProtocol.BrokerInfo>(count);
                    Set<Integer> brokerIds = new HashSet<>();
                    for (int i = 0; i < count; i++) {
                        int broker = getInt(source); if (!brokerIds.add(broker)) throw invalid("Duplicate broker");
                        brokers.add(new ClusterProtocol.BrokerInfo(broker,
                                new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint(getString(source, 255), getInt(source)),
                                getLong(source), getBoolean(source)));
                    }
                    int topicsCount = getCount(source, clusterLimits.maxTopics(), 24); var topics = new ArrayList<ClusterProtocol.TopicInfo>(topicsCount);
                    Set<UUID> topicIds = new HashSet<>(); Set<String> names = new HashSet<>(); int totalPartitions = 0;
                    for (int i = 0; i < topicsCount; i++) {
                        String name = getString(source, MAX_STRING); UUID topic = getUuid(source);
                        if (!topicIds.add(topic) || !names.add(name)) throw invalid("Duplicate topic");
                        int partitionsCount = getCount(source, clusterLimits.maxPartitions(), 38); totalPartitions = Math.addExact(totalPartitions, partitionsCount);
                        if (totalPartitions > clusterLimits.maxPartitions()) throw invalid("Too many partitions");
                        var partitions = new ArrayList<ClusterProtocol.PartitionInfo>(partitionsCount); Set<Integer> ids = new HashSet<>();
                        for (int j = 0; j < partitionsCount; j++) {
                            int partition = getInt(source); if (!ids.add(partition)) throw invalid("Duplicate partition");
                            Error state = getError(source); int replicaCount = getCount(source, 1, 4);
                            var replicas = new ArrayList<Integer>(replicaCount);
                            for (int k = 0; k < replicaCount; k++) replicas.add(getInt(source));
                            int leader = getInt(source); long leaderEpoch = getLong(source), partitionEpoch = getLong(source);
                            if (!brokerIds.contains(leader)) throw invalid("Unknown partition leader");
                            partitions.add(new ClusterProtocol.PartitionInfo(partition, state, replicas, leader, leaderEpoch, partitionEpoch));
                        }
                        topics.add(new ClusterProtocol.TopicInfo(name, topic, partitions));
                    }
                    yield new ClusterProtocol.MetadataReply(error, cluster, offset, brokers, topics);
                }
                case 3 -> {
                    int count = getCount(source, limits.maxPartitionEntries(), 43); var results = new ArrayList<ClusterProtocol.ProduceResult>(count);
                    Set<TopicPartition> unique = new HashSet<>();
                    for (int i = 0; i < count; i++) {
                        var partition = getPartition(source); if (!unique.add(partition)) throw invalid("Duplicate partition");
                        var state = getError(source); int outcome = Byte.toUnsignedInt(getByte(source));
                        if (outcome > 2) throw invalid("Invalid Produce outcome");
                        results.add(new ClusterProtocol.ProduceResult(partition, state, ClusterProtocol.Outcome.values()[outcome],
                                getLong(source), getLong(source)));
                    }
                    yield new ClusterProtocol.ProduceReply(error, results);
                }
                case 4 -> {
                    int count = getCount(source, limits.maxPartitionEntries(), 54); var results = new ArrayList<ClusterProtocol.FetchResult>(count);
                    Set<TopicPartition> unique = new HashSet<>();
                    for (int i = 0; i < count; i++) {
                        var partition = getPartition(source); if (!unique.add(partition)) throw invalid("Duplicate partition");
                        var state = getError(source); long start = getLong(source), end = getLong(source), high = getLong(source);
                        int batchesCount = getCount(source, limits.maxFrameBytes() / 42, 42); var batches = new ArrayList<FetchBatch>(batchesCount);
                        for (int j = 0; j < batchesCount; j++) batches.add(new FetchBatch(getLong(source), WireBatchCodec.decode(getWireBatch(source), limits)));
                        results.add(new ClusterProtocol.FetchResult(partition, state, start, end, high, batches));
                    }
                    yield new ClusterProtocol.FetchReply(error, results);
                }
                default -> throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
            };
            requireConsumed(source); return new ResponseFrame(operation, (short) 2, id, body);
        } catch (IllegalArgumentException | ArithmeticException bad) {
            throw new ProtocolException(ErrorCode.INVALID_REQUEST, "Invalid cluster response", bad);
        }
    }

    private static void putRoute(DataOutputStream out, ClusterProtocol.Route route) throws IOException {
        putPartition(out, route.partition()); out.writeInt(route.brokerId()); out.writeLong(route.brokerEpoch()); out.writeLong(route.leaderEpoch());
    }
    private static ClusterProtocol.Route getRoute(ByteBuffer source) throws ProtocolException {
        return new ClusterProtocol.Route(getPartition(source), getInt(source), getLong(source), getLong(source));
    }
    private static boolean getBoolean(ByteBuffer source) throws ProtocolException {
        int value = Byte.toUnsignedInt(getByte(source)); if (value > 1) throw invalid("Invalid boolean"); return value == 1;
    }

    private byte[] encode(short operation, short version, long requestId, Writer writer)
            throws IOException {
        if (requestId < 0)
            throw invalid("Negative request ID");
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
        if (source.getInt() != frame.length - 4)
            throw invalid("Frame length mismatch");
        return source;
    }

    private static void requireHeader(short operation, short version, long id)
            throws ProtocolException {
        if (version != VERSION && version != 2)
            throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, "Unsupported version");
        if (id < 0)
            throw invalid("Negative request ID");
        if (operation < 1 || operation > 4)
            throw new ProtocolException(ErrorCode.UNSUPPORTED_OPERATION, "Unknown operation");
    }

    private static void requireOperation(short actual, int expected) throws ProtocolException {
        if (actual != expected)
            throw invalid("Operation/body mismatch");
    }

    private static void requireConsumed(ByteBuffer source) throws ProtocolException {
        if (source.hasRemaining())
            throw invalid("Trailing frame bytes");
    }

    private static void putArrayCount(DataOutputStream out, int count, int max)
            throws IOException {
        if (count < 0 || count > max)
            throw invalid("Array count exceeds limit");
        out.writeInt(count);
    }

    private static void putString(DataOutputStream out, String value, int max)
            throws IOException {
        Objects.requireNonNull(value);
        byte[] encoded = strictUtf8(value);
        if (encoded.length > max)
            throw invalid("String exceeds limit");
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
        if (size < 0 || size > max || size > source.remaining())
            throw invalid("Invalid string length");
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
        if (partition < 0)
            throw invalid("Negative partition");
        return new TopicPartition(id, partition);
    }

    private static void putError(DataOutputStream out, Error error) throws IOException {
        out.writeShort(error.code().number());
        putString(out, error.message(), 512);
    }

    private static Error getError(ByteBuffer source) throws ProtocolException {
        return new Error(ErrorCode.fromNumber(getShort(source)), getString(source, 512));
    }

    // Mirrors the size checks of WireBatchCodec.decode so an oversized batch is rejected before
    // its bytes are copied.
    private byte[] getWireBatch(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 14)
            throw invalid("Incomplete wire batch header");
        int length = source.getInt(source.position() + 2);
        if (length < 34 || length > source.remaining())
            throw invalid("Invalid wire batch length");
        if ((long) length + 8 > limits.maxWireBatchBytes()
                || (long) length + 16 > limits.maxStorageBatchBytes())
            throw new ProtocolException(ErrorCode.BATCH_TOO_LARGE, "Wire batch exceeds limit");
        byte[] bytes = new byte[length];
        source.get(bytes);
        return bytes;
    }

    // minBytes is the smallest encoding of one element, so a count that cannot fit in the
    // remaining bytes is rejected before a list of that size is allocated.
    private static int getCount(ByteBuffer source, int max, int minBytes) throws ProtocolException {
        int count = getInt(source);
        if (count < 0 || count > max || count > source.remaining() / minBytes)
            throw invalid("Invalid array count");
        return count;
    }

    private static byte getByte(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 1)
            throw invalid("Incomplete byte");
        return source.get();
    }

    private static short getShort(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 2)
            throw invalid("Incomplete short");
        return source.getShort();
    }

    private static int getInt(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 4)
            throw invalid("Incomplete integer");
        return source.getInt();
    }

    private static long getLong(ByteBuffer source) throws ProtocolException {
        if (source.remaining() < 8)
            throw invalid("Incomplete long");
        return source.getLong();
    }

    private static ProtocolException invalid(String reason) {
        return new ProtocolException(ErrorCode.INVALID_REQUEST, reason);
    }

    private static ProtocolException asProtocol(IOException error) {
        return error instanceof ProtocolException known
                ? known
                : new ProtocolException(ErrorCode.INVALID_REQUEST, "Codec I/O error", error);
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }
}

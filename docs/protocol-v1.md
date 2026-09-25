# Broker binary protocol v1

Phase 2 uses a dedicated TCP protocol. It does not implement Kafka's wire format. All multi-byte integers use big-endian encoding. Unknown operations and versions receive an error if the frame header is valid; malformed framing closes the connection.

## Envelope

`int32 frameLength` counts bytes after the length field. It must be between 12 and 8 MiB. The remaining header is `int16 operation`, `int16 version=1`, `int64 requestId >= 0`, then a body. A response echoes the operation, version, and request ID. Different responses on a connection may arrive out of order. IDs must be unique while in flight and cannot be reused during a connection lifetime.

String = `int32 utf8ByteLength` followed by strict UTF-8 bytes. Array = `int32 count` followed by elements. Null strings and negative counts are invalid. UUID = `int64 mostSignificantBits` and `int64 leastSignificantBits`. A partition reference = UUID plus `int32 partitionId`. Fields must consume exactly the body; extra bytes are invalid.

| Operation | ID | Request body | Successful response body |
| --- | ---: | --- | --- |
| CreateTopic | 1 | string name, int32 partitionCount | UUID topicId |
| Metadata | 2 | array<string> names; empty means list all | string host, int32 port, array<TopicInfo> |
| Produce | 3 | int8 ack, int32 timeoutMs, array<ProduceEntry> | array<ProduceResult> |
| Fetch | 4 | int32 maxBytes, int32 minBytes, int32 maxWaitMs, array<FetchEntry> | array<FetchResult> |

All response bodies begin with `Error = int16 code + string message`. NONE (0) requires an empty message. If the top-level code is nonzero, the response body ends after Error. An entry-specific error is contained in the corresponding result; successful entries in the same request remain successful. Error text is for diagnostics and capped at 512 UTF-8 bytes; clients use numeric codes.

`TopicInfo = string name + UUID id + array<PartitionInfo>`. `PartitionInfo = int32 id + Error`.

`ProduceEntry = partition reference + wire batch`. Ack byte 0=APPENDED and 1=FLUSHED. `ProduceResult = partition reference + Error + int64 firstOffset + int64 nextOffset`; both offsets are -1 for an entry error. A request contains at most one entry per partition and uses one ack mode. Requests spanning partitions are not atomic.

`FetchEntry = partition reference + int64 offset + int32 partitionMaxBytes`. `FetchResult = partition reference + Error + int64 logStartOffset + int64 logEndOffset + array<FetchBatch>`; start/end are -1 on entry error. `FetchBatch = int64 baseOffset + wire batch`. The first batch returned for an entry may begin before the requested record offset; the client filters earlier records. Fetch reads through logEndOffset, including batches not yet flushed. Only the first data batch of a whole response may exceed maxBytes or its partitionMaxBytes, and then the response contains no other data batch. The 8 MiB frame cap remains absolute.

## Wire batch

Wire batch header has 14 bytes: `int16 version=1` at 0; `int32 totalLength` at 2 (header inclusive); `int32 recordCount` at 6; `int32 CRC32C` at 10; record payload begins at 14. CRC32C covers bytes `[0,10)` and `[14,totalLength)`. The payload uses the Phase 1 record encoding: timestamp int64; nullable key/value as int32 length and bytes (-1 means null); header count int32; ordered headers with UTF-8 key and nullable byte-array value. Empty byte arrays differ from null, and duplicate header keys preserve order. No client-supplied offset or compression.

One storage batch uses payload+30 bytes; Produce wire batch uses payload+14; Fetch contributes payload+22 including baseOffset. A batch must satisfy both the 1 MiB wire limit including baseOffset and the 1 MiB storage batch limit. Record count is at most 10,000. Partition entries per request are at most 64.

## Errors

| Code | Number | Code | Number |
| --- | ---: | --- | ---: |
| NONE | 0 | INVALID_REQUEST | 1 |
| UNSUPPORTED_OPERATION | 2 | UNSUPPORTED_VERSION | 3 |
| UNKNOWN_TOPIC | 4 | UNKNOWN_PARTITION | 5 |
| TOPIC_ALREADY_EXISTS | 6 | OFFSET_OUT_OF_RANGE | 7 |
| BATCH_TOO_LARGE | 8 | OVERLOADED | 9 |
| REQUEST_TIMED_OUT | 10 | PARTITION_UNAVAILABLE | 11 |
| STORAGE_ERROR | 12 | BROKER_SHUTTING_DOWN | 13 |

Timeout or a lost response after sending Produce may have an unknown write outcome. Retrying can append a duplicate. Request IDs correlate responses but do not deduplicate writes. APPENDED confirms local append; FLUSHED confirms the Phase 1 local durability marker covers the batch. Neither is replication acknowledgment.

Golden frames for CreateTopic, Produce, and Fetch are in `src/test/resources/protocol/` and asserted independently of round-trip tests.

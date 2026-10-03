# Data protocol v2

All integers are big-endian. The envelope remains `frameLength i32` (excluding
itself), `operation i16`, `version i16=2`, `requestId i64`. Request IDs correlate
replies and do not deduplicate writes. Operations 1–4 retain their v1 meaning.
Production cluster brokers reject v1 requests; historical v1 codecs/vectors remain.

UUIDs occupy 16 bytes. Strings are i32 byte length plus strict UTF-8 (topic names
249 bytes, hosts 255, errors 512). Arrays are i32 count plus elements. Booleans are
u8 0/1. Counts and remaining-byte bounds are checked before allocation; trailing
bytes and unknown outcomes are rejected. Defaults: frame 8 MiB, batch 1 MiB,
64 request partition entries, 32 brokers, 128 topics, 1024 metadata partitions.
Metadata count limits follow the configured `MetadataLimits`. Wire batch v1 is
unchanged.

`partition` is topic UUID followed by partition ID i32. `route` is partition,
broker ID i32, broker epoch i64, leader epoch i64. A route must match the committed
local session and assignment, serving gate and provisioned runtime before enqueue
and again on the partition lane before I/O. No gate monitor is held during I/O.

| Op | Request payload after envelope |
|---|---|
| 1 CreateTopic | cluster UUID, name, partition count i32, RF i16=1, timeoutMs i32 |
| 2 Metadata | cluster UUID, array of names |
| 3 Produce | cluster UUID, ack u8 (APPENDED=0/FLUSHED=1), timeoutMs i32, array of route + wire batch |
| 4 Fetch | cluster UUID, maxBytes/minBytes/maxWaitMs i32, array of route + offset i64 + maxBytes i32, allowOversizedFirstBatch bool |

Only bootstrap Metadata accepts the zero cluster UUID. Other requests require
the configured cluster UUID. Produce/CreateTopic timeout is 1–30000 ms; Fetch
wait is 0–5000 ms and aggregate batch budget is at most 4 MiB. One original
processing deadline applies across partition entries. Duplicate partition entries
are invalid.

Every reply starts with error code i16 and message string. A nonzero top-level
error ends the body and decodes as `Protocol.Failure`, echoing the envelope.

| Op | Success payload after error |
|---|---|
| 1 | topic UUID, committed metadata offset i64 |
| 2 | cluster UUID, applied offset i64, broker array, topic array |
| 3 | array of partition + error + outcome u8 + firstOffset/nextOffset i64 |
| 4 | array of partition + error + start/LEO/HW i64 + array of baseOffset i64 + wire batch |

Broker descriptor: ID i32, host string, port i32, broker epoch i64, fenced bool.
Topic descriptor: name, UUID, partition array. Partition descriptor: ID i32,
error, replica ID array, leader ID i32, leader epoch i64, partition epoch i64.
RF1 assignments contain one replica equal to the leader. Epochs remain distinct.

Produce outcomes: SUCCESS=0 requires NONE and valid exclusive offsets;
REJECTED=1 proves append never started; UNKNOWN=2 means append may have occurred.
Both failure outcomes require a nonzero error and offsets -1. A deadline or
failure after mutation starts is UNKNOWN, even if the I/O later succeeds. A
FLUSHED waiter fenced after append is UNKNOWN. Clients must not automatically
retry UNKNOWN; REJECTED alone is not enough without a retryable code/deadline.

RF1 HW is the local durable end, captured with start/LEO and batches on one lane.
Fetch reads only batches ending at or below HW. The first batch may precede the
requested offset, preserving v1 filtering. Only an explicitly delegated first
batch of the whole operation may exceed the budget; v2 defaults this flag false.
Long polling wakes when flush advances the durable prefix or applied fencing
revokes permission. Heartbeat timeout alone does not revoke serving permission.

Additional data errors: CLUSTER_MISMATCH=16, STALE_BROKER_EPOCH=23,
NO_ELIGIBLE_BROKER=24, NOT_PARTITION_LEADER=25, FENCED_BROKER=26,
STALE_PARTITION_EPOCH=27. Numbers 0–13 keep their v1 meanings.

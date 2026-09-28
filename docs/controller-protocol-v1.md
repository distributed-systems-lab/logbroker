# Controller protocol v1

Big-endian integers; strings/blobs use an i32 byte length and strict UTF-8 for strings. Negative counts, unsupported tags/versions, overflow and trailing bytes are rejected. Arrays must fit their count, configured cap and minimum element size before allocation. Controller operations are distinct from broker data operations.

Frame: length i32 excluding prefix; operation i16; version i16=1; direction u8 (request=0,response=1); cluster UUID; sender i32 (-1 for admin); requestId i64; canonical voter SHA-256 (32 bytes); body; CRC32C i32. CRC covers everything after length through body. Minimum frame length excluding prefix: 69 bytes. Request IDs correlate within a connection incarnation. Peer identity/voter hash is validated before changing consensus state.

Reply prefix: error i16, message string (max 512 bytes), epoch i64, leader ID i32. A nonzero error ends the body. Shared errors retain numbers 0–13 of broker v1. Additional controller errors: NOT_LEADER=14, STALE_EPOCH=15, CLUSTER_MISMATCH=16, INCONSISTENT_VOTER_SET=17, NODE_UNAVAILABLE=18, SNAPSHOT_NOT_FOUND=19.

| Operation | Request | Success response after prefix |
|---|---|---|
| Vote 101 | epoch i64, last epoch i64, end i64 | granted u8 |
| BeginQuorumEpoch 102 | epoch i64 | empty |
| EndQuorumEpoch 103 | epoch i64 | empty |
| QuorumFetch 104 | epoch,end,lastEpoch i64; maxBytes,maxWaitMs i32; challenge i64 | challenge,commit i64; kind u8; payload |
| FetchSnapshot 105 | epoch i64; SnapshotId; position i64; maxBytes i32 | SnapshotId; position,totalLength i64; chunk blob; chunk CRC32C i32 |
| DescribeQuorum 106 | empty | node i32, role u8, epoch i64, leader i32, generation UUID, append/durable/commit/apply/snapshot i64, ready u8, replica array(id i32,end i64), failure string |
| CreateTopic 107 | name string, partitions i32, timeoutMs i32 | topic UUID |
| ReadMetadata 108 | timeoutMs i32 | MetadataView |
| ReadLocalMetadata 109 | empty | MetadataView |

SnapshotId: exclusive end i64, last included epoch i64, content UUID. Fetch kind DATA=0 contains an array of batches; DIVERGENCE=1 carries commonEpoch/commonEnd i64; SNAPSHOT=2 carries SnapshotId. Batch encoding: base i64, count i32, length-prefixed entry payloads, CRC32C over preceding batch bytes. Entries use version i16=1, kind u8 (LeaderChange=1,TopicCreated=2,ReadBarrier=3), epoch i64, payload. LeaderChange payload is leader ID i32; TopicCreated uses existing versioned MetadataEventCodec; barrier has no payload.

MetadataView: consistency u8 (LOCAL=0,LINEARIZABLE=1), node i32, epoch i64, leader i32, commit/apply i64, array of length-prefixed TopicCreated events. Controller metadata has no broker endpoint or partition availability. Local view can be stale; linearizable view waits for a newly admitted log barrier to commit/apply.

Fetch limits include encoded batch bytes and preserve whole leader batches. Metadata batch <=1 MiB, frame <=8 MiB, response data budget <=4 MiB. Snapshot chunks <=256 KiB, total snapshot <=64 MiB. Array preflight precedes full batch/chunk decoding; charged decoded memory includes at least twice raw frame length plus 64 bytes per decoded element. Frame CRC does not replace batch/file/chunk checksums.

Golden DescribeQuorum request (cluster UUID low half=1, admin sender, request ID=7, zero hash for codec-only fixture): see `src/test/resources/controller/protocol-v1-vectors.txt`. A real transport rejects that zero hash when it does not match configured membership.

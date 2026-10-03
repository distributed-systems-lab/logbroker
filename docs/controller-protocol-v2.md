# Controller control protocol v2

Frames retain length prefix, operation and CRC32C from v1. The v2 fixed envelope is:
operation i16, version i16=2, response u8, cluster UUID (16 bytes), senderId i32,
senderRole u8, requestId i64, voterHash (32 bytes), body, frame CRC32C i32.
The minimum frame length after the length prefix is 70 bytes (74 including prefix).

Sender roles: ADMIN=0, VOTER=1, BROKER=2. Voter IDs must be in fixed membership and
carry its canonical SHA-256 hash. Admin/broker hash bytes are zero. Admin senderId
is -1; broker ID is nonnegative and has its own namespace. Role, ID, schema and
broker incarnation are pinned to the connection. Every frame requires the cluster
UUID. This is identity validation, not authentication.

Requests allowed by role: ADMIN 106–109, VOTER 101–109, BROKER 106–113. Only VOTER
emits replies, including replies for 110–113. Broker requests enter the bounded
shared service/queue and never voter contact tracking or reserved voter byte pools.
Response correlation includes operation, version, request ID and socket incarnation.

Session is brokerId i32, storage UUID, incarnation UUID, brokerEpoch i64. Strings use
i32 UTF-8 byte length. Existing ReplyMeta prefix is unchanged; nonzero error ends
the body. Offsets are exclusive.

| Op | Request fields | Success fields following ReplyMeta |
|---|---|---|
| 110 Register | brokerId, storage UUID, incarnation UUID, expectedBrokerEpoch i64 (-1 absent), host, port i32, min/max feature i16, timeoutMs i32 | Session, registrationOffset i64, requiredMetadataOffset i64 |
| 111 Heartbeat | Session, sequence i64, appliedOffset i64, recovery UUID, recoveryComplete u8, timeoutMs i32 | status u8, brokerEpoch i64, stateOffset i64, requiredMetadataOffset i64, recovery UUID |
| 112 ObserverFetch | Session, controllerEpoch i64, nextOffset i64, prefixEpoch i64, maxBytes/maxWaitMs i32 | commitOffset i64, kind u8, data batches (0) or SnapshotId (2) |
| 113 ObserverSnapshot | Session, controllerEpoch i64, SnapshotId, position i64, maxBytes i32 | SnapshotId, position/totalLength i64, chunk blob, chunk CRC32C |

Heartbeat statuses ACTIVE=0, FENCED=1, STALE_SESSION=2, REGISTRATION_REQUIRED=3 retain
committed revision and recovery target under top-level NONE. Ordinary RPC timeout or
NOT_LEADER never implies a local serving lease expiration.

106 v2 success is the existing quorum status followed by canonical voter count/list
(ID, host, port) and 32-byte hash. 107 adds replicationFactor i16 (must be 1) before
timeout and returns topic UUID plus commitOffset. 108/109 return consistency u8,
nodeId i32, commitOffset i64, then an i32-length v2 image; epoch/leader hint remain in
ReplyMeta. The full image includes appliedOffset. Linearizable reads still require a
committed barrier; local reads retain the LOCAL label.

Errors 0–19 retain their v1 meanings. Added: INCOMPATIBLE_METADATA_VERSION=20,
BROKER_ID_IN_USE=21, STORAGE_ID_MISMATCH=22, STALE_BROKER_EPOCH=23,
NO_ELIGIBLE_BROKER=24. CAS error 23 applies to registration; heartbeat refusal uses
structured status. Observer replies may contain only whole committed batches or a
committed snapshot; they cannot request truncation of already applied metadata.

Count/length preflight precedes decoded memory reservation. Frame cap is 8 MiB,
metadata batch 1 MiB, observer data 4 MiB, snapshot chunk 256 KiB and snapshot envelope
64 MiB. Storage-v2 image rules are in [controller-storage-v2.md](controller-storage-v2.md).

The Phase 4 implementation plan wires registration/liveness in Tasks 4–5 and observer
read effects in Task 6. The protocol checkpoint alone does not enable a v2 cluster
runtime. Existing v1 controller roots reject full v2 cluster reads explicitly.

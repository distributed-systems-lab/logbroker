# Controller metadata storage v2

Schema v2 extends quorum entry payloads; the storage record-batch format is unchanged.
All integers use big-endian order. Offsets are exclusive. Entries start with schema i16,
kind u8 and controller epoch i64. V1 kinds 1 (LeaderChange), 2 (legacy TopicCreated)
and 3 (ReadBarrier) retain their original bytes. V2 supports those kinds and adds:

| Kind | Payload in field order |
|---|---|
| 4 FeatureLevel | level i16 (2) |
| 5 BrokerRegistration | brokerId i32, storage UUID, incarnation UUID, brokerEpoch i64, host string, port i32, minVersion i16, maxVersion i16 |
| 6 BrokerState | brokerId i32, brokerEpoch i64, fenced u8 (0/1) |
| 7 TopicRecord | topic UUID, name string, partition count i32 |
| 8 PartitionRecord | topic UUID, partitionId i32, replica count i32, replica IDs i32, leaderId i32, leaderEpoch i64, partitionEpoch i64 |

UUIDs occupy 16 bytes. Strings use an i32 byte length followed by strict UTF-8. Hosts
are at most 255 bytes; topic names retain the 1–249 ASCII name contract. Entries are
bounded by the 1 MiB metadata batch budget. RF=1 requires exactly one replica equal
to leaderId. IDs/epochs are nonnegative and UUIDs nonzero. Incarnation and storage
identity are distinct; brokerEpoch is the exclusive registration record offset.

`MetadataLimits` supplies configurable count limits and a conservative image-size
preflight. Readers reject invalid counts, versions, kinds, booleans, UTF-8, truncation
and trailing bytes before publishing metadata. Topic and all assignment records must
be committed and applied in a single complete batch. No Phase 2/3 root migration is
provided. See the [Phase 4 spec](superpowers/specs/2026-10-02-cluster-phase-4-design.md).

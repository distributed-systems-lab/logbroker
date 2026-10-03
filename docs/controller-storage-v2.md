# Controller metadata storage v2

Schema v2 extends quorum entry payloads; the storage record-batch format is unchanged.

Broker roots use a separate 50-byte identity (`0x42494432`, schema i16=2, total
length i32, cluster UUID, brokerId i32, storage UUID, CRC32C), and `.broker.lock`
held until all ordered I/O drains. Format publishes this identity last, after
`observer/`, `partitions/` and the inventory INIT frame are forced. Opening a root
never creates missing components.

The observer has its own 30-byte `observer-identity.bin` (`0x4f494432`, schema 2,
length 30, cluster UUID, CRC32C), generation directories and `observer-state.journal`.
The journal uses StateJournal framing without HARD_STATE records. GENERATION retains
the existing 25/57-byte payload; observer COMMIT payload is the exclusive end i64,
and SNAPSHOT_SET bytes are unchanged. No vote or controller epoch is persisted there.
`membership.bin` pins the independently verified 32-byte voter hash with a CRC32C
trailer before snapshot acceptance. Its hash and cluster ID validate snapshot headers
without constructing a voter identity. SnapshotJournal borrows journal ownership;
the broker and controller continue to own their separate journals and root locks.

Observer receive validates the entire contiguous committed prefix before writing,
then appends/flushes, forces its actual received-end checkpoint, and publishes the
image. An empty fetch does not checkpoint remote coverage beyond received data.
Restart applies only the checkpoint and discards an unapplied received tail. Install
forces the immutable snapshot and empty replacement log before publishing GENERATION;
cancellation before that frame leaves the old log usable. Cleanup follows publication
and old-log close, preserves referenced/pinned snapshots, and ignores unrelated paths.
Controller `identity.bin` uses explicit format 2, retains the magic `0x51494431`,
and stores metadata.version i16=2 after the local node ID and before canonical voters.
The length and CRC32C cover the entire identity. Format 1 retains its original bytes
and has implicit metadata.version=1; a root cannot be opened with a different version.
Formatting accepts `--metadata-version 2`; startup pins it with `metadata.version=2`.
The first empty-log leader commits LeaderChange and FeatureLevel together before
admission. A later leader applies its committed marker and inherited prefix before
deciding whether an additional feature bootstrap is needed.
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

V2 image payload: magic i32 `0x4d494d32`, schema i16=2, feature level i16,
exclusive applied offset i64, broker count i32 and registrations with fenced u8 and
stateOffset i64, topic count i32 and TopicRecord payloads, partition count i32 and
PartitionRecord payloads. Brokers are sorted by ID; topics by unsigned UUID halves;
partitions by unsigned topic UUID then partition ID. Level zero describes only an
empty cluster before its committed feature initialization. Readers validate complete
assignment references and lifecycle revisions before returning an image.

Snapshot envelope magic and layout stay unchanged; the explicit envelope version
selects the v1 or v2 image decoder. V1 snapshots retain their original encoding.
V2 snapshots preserve feature level, broker identity, fencing revision and partition
epochs. Snapshot size is bounded by configuration (at most 64 MiB); upload chunks
remain bounded independently (at most 256 KiB). Apply and recovery must receive the
same count limits. A corrupt committed batch fails recovery without partial apply.

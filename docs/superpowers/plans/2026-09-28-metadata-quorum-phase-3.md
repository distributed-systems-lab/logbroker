# Metadata Quorum — Phase 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây quorum ba controller độc lập, quản lý topic metadata thật, có election, durable commit, linearizable reads, snapshot và recovery.

**Architecture:** Một event loop sở hữu consensus; disk operations chạy tuần tự và gửi completion có epoch/generation về loop. QuorumLog tái sử dụng PartitionLog; metadata apply tách khỏi replication, transport Netty và broker standalone. Admin client dùng protocol riêng, đọc qua log barrier và retry CreateTopic theo tên/cấu hình.

**Tech Stack:** Java 21, Maven single module, JUnit Jupiter 5.11.4, Netty 4.2.18.Final hiện có. Namespace `vn.huyqt.logbroker`. Không dùng thư viện Raft hoặc Kafka client.

**Spec:** [Phase 3 đã duyệt ngày 2026-09-28](../specs/2026-09-28-metadata-quorum-phase-3-design.md). Đọc cả spec và plan trước thực hiện.

## Global Constraints

- Ba controller với membership cố định; không mở data partition hoặc tích hợp broker trong Phase 3.
- Protocol/client riêng; không thay đổi số mã hoặc wire format của Phase 2.
- Metadata quorum dùng Raft; không tạo Raft group cho từng data partition.
- Mọi mốc offset dùng quy ước exclusive; mốc append, durable, commit, apply và snapshot nằm trên batch boundary.
- `snapshotEndOffset <= appliedOffset <= commitOffset <= durableEndOffset <= logEndOffset` trong active generation.
- Một vote mỗi epoch; persist trước phản hồi. Commit cần local durable, majority durable matching prefix và current-epoch rule.
- Core không dùng Netty types; không disk I/O hoặc chờ future trên network/consensus event loop.
- Format tường minh; không tự format missing storage hoặc tái sử dụng voter ID sau mất đĩa.
- Recovery nghiêm ngặt; không suy ra commit từ log end, không bỏ qua corruption đã công bố.
- Process-kill test không chứng minh an toàn trước mất điện.
- Membership động, thay thế voter mất toàn bộ storage, rolling upgrade, TLS/authentication và tích hợp broker nằm ngoài Phase 3.
- Voters/quorum: 3 / 2. Fetch idle wait / RPC timeout: 100 ms / 1 s. Election timeout ngẫu nhiên: 1.5–3 s. Leader mất liên lạc đa số: 3 s.
- Admin deadline / shutdown deadline: 30 s / 30 s. Metadata batch / RPC frame tối đa: 1 MiB / 8 MiB. Fetch response data budget: 4 MiB, luôn đủ một batch hợp lệ.
- Snapshot chunk / snapshot file tối đa: 256 KiB / 64 MiB. Snapshot trigger: 16 MiB log mới kể từ snapshot trước. Snapshot transfer: Một download và tối đa hai upload mỗi node.
- Pending admin requests: 1,024 mỗi node. Event queue / disk task queue: 4,096 / 256. Tổng inbound/outbound bytes đang giữ: 64 MiB mỗi hướng, mỗi node.
- Topics / tổng partitions: 128 / 1,024, kế thừa Phase 2.

## Bối cảnh và trình tự

Root: `D:/User2/distributed-system-labs/java-log-broker`. Không sửa checkout `../kafka`. Baseline được khảo sát tại commit `2a54425`; xác minh lại HEAD và working tree khi execution bắt đầu. Chỉ tạo worktree khi thực thi, không cần cho việc viết plan. Giữ một plan vì các subsystem cùng thực hiện một contract consensus/persistence; mỗi task có test riêng, không triển khai toàn phase trước khi review.

Hiện tại PartitionLog chỉ mở từ offset 0, logStartOffset trả 0, LogRecovery bắt đầu scan tại 0. truncateTo sửa nhiều file trực tiếp. Vì thế snapshot cần extension có giới hạn, và QuorumLog phải journal truncate intent trước khi gọi storage mutation. TopicCatalog/MetadataEventCodec dùng lại được mà không kéo PartitionRegistry vào controller. ClientTransport/ServerTransport gắn với DTO/dispatcher Phase 2 nên không tái sử dụng interface đó cho quorum. DeadlineScheduler, ResourceBudget và BoundedFrameDecoder có thể dùng lại có điều chỉnh nhỏ, không refactor toàn bộ broker.

Trước Task 1 chạy `mvn clean verify`; ghi nhận số tests, HEAD và lỗi môi trường. Mỗi nhóm test là một vòng đỏ → thay đổi nhỏ → xanh. Lỗi dependency/network không phải RED hợp lệ. Snippet dưới đây là phần lõi test/thuật toán; bổ sung imports, lifecycle cleanup và toàn bộ acceptance cases được ghi trong task. Không sửa code trong lượt viết plan.

## Bản đồ file

Prefix thay thế chính xác: `M/` = `src/main/java/vn/huyqt/logbroker/`; `T/` = `src/test/java/vn/huyqt/logbroker/`; `R/` = `src/test/resources/`. Class production mới nằm trong `controller/` trừ storage primitive và decoder overload.

| Khu vực | Files chính | Vai trò |
|---|---|---|
| controller/ | ControllerConfig, ClusterIdentity, ControllerNode, ControllerMain, ControllerService | Composition, config, admin admission |
| controller/persistence/ | DurableFiles, StateJournal, QuorumStateStore, GenerationStore | Publication, vote/commit checkpoint, generation |
| controller/log/ | QuorumEntry, QuorumEntryCodec, QuorumBatch, QuorumLog, EpochIndex | Replicated log và reconciliation |
| controller/consensus/ | QuorumStateMachine, QuorumEvent, QuorumEffect, QuorumStatus, ReplicationTracker | Pure transitions |
| controller/runtime/ | ControllerLoop, OrderedDiskExecutor, EffectRunner | Bounded scheduling và effects |
| controller/metadata/ | MetadataStateMachine, MetadataImage, MetadataImageCodec | Catalog đã commit |
| controller/snapshot/ | SnapshotId, SnapshotStore, SnapshotCoordinator, SnapshotTransfer | Snapshot lifecycle |
| controller/protocol/ | QuorumProtocol, QuorumError, QuorumCodec | Envelope, bodies và validation |
| controller/transport/ | QuorumTransport, NettyQuorumTransport | Peer/admin I/O boundary |
| controller/client/ | ControllerClient, ControllerClientException, ControllerCli | Bootstrap, retry, CLI |
| T/controller/support/ | QuorumHarness, FakeDisk, SimulatedTransport, FaultFiles, ThreeControllerProcesses | Deterministic và process fixtures |

## Hợp đồng dùng chung

Định nghĩa các kiểu sau đúng một lần trong task chỉ định; không tạo một bộ DTO khác cho harness. Methods disk ở dưới đều `throws IOException` và chỉ chạy trên disk worker.

```java
// Task 1: controller.log
sealed interface QuorumEntry {
    long epoch();
    record LeaderChange(long epoch, int leaderId) implements QuorumEntry {}
    record Topic(long epoch, TopicCatalog.TopicCreated event) implements QuorumEntry {}
    record ReadBarrier(long epoch) implements QuorumEntry {}
}
record QuorumBatch(long baseOffset, List<QuorumEntry> entries) {
    long nextOffset() { return Math.addExact(baseOffset, entries.size()); }
}
// Task 1: controller
record ClusterIdentity(UUID clusterId, int nodeId, List<Voter> voters) {
    record Voter(int id, String host, int port) {}
}
// Task 4: log metadata immutable, no disk calls from event loop
record LogPosition(long endOffset, long lastEpoch) {} // nested EpochIndex
// Task 5: snapshot
record SnapshotId(long endOffset, long lastEpoch, UUID contentId) {}
// Task 7: consensus
record QuorumStatus(int nodeId, Role role, long epoch, int leaderId,
    UUID generation, long logEnd, long durableEnd, long commit, long applied,
    long snapshotEnd, boolean ready, Map<Integer, Long> durableMatches,
    String failure) {
    enum Role { UNATTACHED, FOLLOWER, CANDIDATE, LEADER, FAILED, STOPPING }
}
```

All cross-package types/methods shown are public in implementation; compact snippets omit access modifiers/imports. All records defensively copy collections/byte arrays; validate arithmetic overflow and field ranges before allocation. Epoch starts 0, first candidate epoch 1, empty prefix lastEpoch=0; absence of leader/vote/admin sender uses -1. Voter IDs are nonnegative. Treat exhausted epoch/offset/request ID as fatal or reconnect before reuse, never wrap.

`ControllerConfig.defaults(ClusterIdentity identity)` produces immutable config with spec defaults, `logConfig()` delegates LogConfig with 64 MiB segments/1 MiB batches; `validate()` checks all relationships. Tests use a builder `ControllerConfig.builder(identity)` with named setters matching table properties and `build()` for small limits/timeouts. Snapshot maximum encoded image bound is `512 + maxTopics * (32 + 249)` bytes for v1 topic-only image, using checked long arithmetic; require configured snapshot max >= bound.

### Wire v1, exact field order (Task 6)

Big endian. `i16/i32/i64` signed; UUID two i64; boolean u8 0/1; string = i32 byte length + strict UTF-8, no null; arrays i32 count. Decoder rejects trailing bytes, negative lengths, unknown tags, invalid bool and checked-size overflow. Hosts <=255 bytes, topic name <=249 ASCII bytes, error/failure text <=512 bytes.

Frame: `i32 length` excluding prefix; `i16 operation, i16 version=1, u8 direction(0=request,1=response), UUID clusterId, i32 senderId, i64 requestId, bytes32 voterSetHash, body, i32 CRC32C`. CRC covers bytes after length through end of body. Fixed overhead excluding length is 69 bytes plus body. Hash is SHA-256 of canonical voter list sorted by ID: count + (id,host,port), no node-specific ID. Admin sender=-1; admin knows same list. Peer-only operations reject sender=-1 before state transitions. One listener supports both, without treating admin as voter.

Responses start `i16 error, string message, i64 epoch, i32 leaderId`; nonzero error ends body. Codes 0..13 retain Phase 2 numbers for shared meanings; new controller-only enum adds NOT_LEADER=14, STALE_EPOCH=15, CLUSTER_MISMATCH=16, INCONSISTENT_VOTER_SET=17, NODE_UNAVAILABLE=18, SNAPSHOT_NOT_FOUND=19. Unknown code is protocol failure; do not extend Phase 2 ErrorCode merely to decode controller messages.

| Op | Request body after envelope | Success body after response prefix |
|---|---|---|
| Vote 101 | epoch:i64,lastEpoch:i64,end:i64 | granted:bool |
| BeginQuorumEpoch 102 | epoch:i64 | empty |
| EndQuorumEpoch 103 | epoch:i64 | empty |
| QuorumFetch 104 | epoch:i64,end:i64,lastEpoch:i64,maxBytes:i32,maxWaitMs:i32,challenge:i64 | challenge:i64,commit:i64,kind:u8,payload |
| FetchSnapshot 105 | epoch:i64,id:SnapshotId,position:i64,maxBytes:i32 | id,position:i64,totalLength:i64,chunk:bytes,chunkCRC:i32 |
| DescribeQuorum 106 | empty | status fields in QuorumStatus order; role:u8, ready:bool; matches sorted (id:i32,end:i64) |
| CreateTopic 107 | name:string,partitions:i32,timeoutMs:i32 | topicId:UUID |
| ReadMetadata 108 | timeoutMs:i32 | MetadataView |
| ReadLocalMetadata 109 | empty | MetadataView |

SnapshotId = end:i64,lastEpoch:i64,contentId:UUID. Fetch kind DATA=0: array of batches; DIVERGENCE=1: commonEpoch:i64,commonEnd:i64; SNAPSHOT=2: SnapshotId. Batch = base:i64,count:i32, followed by count length-prefixed QuorumEntry bytes and batchCRC:i32 covering base/count/entries. Budget includes all encoded batch bytes; max storage batch encoded size also <=1 MiB. Bound count by both minimum entry bytes and remaining buffer before materializing. Preserve leader batch boundaries.

MetadataView = consistency:u8 (LOCAL=0,LINEARIZABLE=1),nodeId:i32,epoch:i64,leaderId:i32,commit:i64,applied:i64,array of TopicCreated payloads sorted by name. No synthesized partition availability or broker endpoints. Error responses carry no success payload. requestId is per connection incarnation and echoed; peer epoch in a valid correlated response may force step-down, but stale traffic never extends liveness.

Fetch challenge is a leader-issued monotonically increasing nonce: response carries current nonce; subsequent follower request echoes it after receiving the response. Count peer as recently live only for a nonce issued within the 3 s window and never count a replay twice. Durable matching progress and recent-contact evidence are separate; a response with data is not evidence follower flushed it.

### Durable layout and crash ordering (Tasks 2–5, 12)

```text
controller-root/
  .lock
  identity.bin                 immutable v1, cluster/node/voters/checksum
  quorum-state.journal         ordered durable state frames
  generations/<UUID>/log/      ordinary storage-v1 segments and indexes
  snapshots/<UUID>.snapshot   immutable v1 image; temporary files use .partial
```

Journal frame = magic:i32(0x514A4E31),version:i16(1),totalLength:i32,sequence:i64,type:i16,payload,CRC32C:i32. CRC covers header+payload. Total length <=64 KiB. Types HARD_STATE=1 (epoch:i64,votedFor:i32), GENERATION=2 (UUID,logStart:i64,snapshotId or bool=false), COMMIT=3 (generation UUID,end:i64), TRUNCATE_INTENT=4 (generation UUID,end:i64), TRUNCATE_DONE=5 (intent sequence:i64), SNAPSHOT_SET=6 (array of at most 2 IDs), PREFIX_INTENT=7 (generation UUID,newStart:i64), PREFIX_DONE=8 (intent sequence:i64). Every journal publication flushes before returning; no rename-overwrite for votes/checkpoints. Journal compaction is outside Phase 3; cap journal at 64 MiB and fail admission/participation explicitly when capacity is exhausted, never delete voting history. Document this bounded lab limitation; adding journal compaction is a future maintenance change.

Identity file = magic:i32(0x51494431),version:i16(1),totalLength:i32,clusterId:UUID,nodeId:i32,canonical voter array,CRC32C:i32. CRC covers all preceding bytes. GENERATION snapshot field is always bool hasSnapshot followed by SnapshotId only when true. Log generation UUID and snapshot content UUID are local immutable file names, validated before resolving paths. Journal integer type constants belong to StateJournal; payload encode/decode helpers belong to QuorumStateStore/GenerationStore. Normal formatting creates generation at offset 0 through existing storage before Task 4 adds nonzero recovery.

Recovery accepts only a structurally valid incomplete final frame as torn write; complete CRC failure is corruption. Replay latest hard state globally, latest published generation and its commit/prefix/truncate intents. Never reset hard state on generation change. Newly installed snapshot provides minimum commit S even without later COMMIT frame. Checkpoint ahead of physical coverage fails. Journal byte position/sequence continuity is verified.

1. Format: create exclusively, write identity + initial generation/log + initial HARD_STATE(0,-1)/GENERATION, force files and relevant directories, then publish usable identity last. A crash-interrupted format is not a valid node and requires explicit cleanup of that new directory.
2. New files: force contents, then persist parent directory entries before journal references them. Journal append force is the publication point. Unsupported directory durability must fail clearly; do not equate `ATOMIC_MOVE` with persistence.
3. Log rollover/flush: in strict controller mode flush data and newly created segment directory entries before reporting durableEnd. Phase 2 open overload retains its existing behavior.
4. Truncate: pause new disk submissions for old generation; persist TRUNCATE_INTENT, remove suffix descending, truncate target, force files/directories, persist DONE. On restart replay incomplete intent before ordinary strict log recovery. Intent may never target below known commit/snapshot. Complete corrupt bytes before the target still fail.
5. Prefix: after two published snapshots, select whole sealed segments ending <= older snapshot. Persist PREFIX_INTENT(new first segment base), delete older files, sync directory, persist DONE. Recovery uses intent to finish deletion before scanning at newStart, so leftover obsolete files cannot masquerade as active data.
6. Snapshot install: verify full temp file; publish immutable snapshot file; create/force empty log at S in fresh generation; drain old I/O; append/force GENERATION(new,S,id); only then switch memory image and ACK progress. Old generation can be removed after publication and reference drain. A snapshot-only generation starts with one retained snapshot; wait for a second before ordinary prefix retention.

`DurableFiles` is a filesystem boundary, not an assertion that Java guarantees identical directory persistence on every provider. Task 2 must establish support on the target filesystem and document it. If native Windows provider cannot satisfy strict publication with standard Java, report that finding at checkpoint and use a verified supported filesystem/runtime (for example a separately verified Linux environment) for strict acceptance; do not silently weaken durability or add a native dependency. Platform change/native support needs a concrete follow-up decision, while pure core tests can proceed. [FileChannel.force](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html#force(boolean)) and [Files.move](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#move(java.nio.file.Path,java.nio.file.Path,java.nio.file.CopyOption...)) define the Java APIs, not a blanket multi-file crash guarantee.

## Tasks

### Task 1: Metadata entries, configuration và deterministic image

**Files:** Create `M/controller/ClusterIdentity.java`, `M/controller/ControllerConfig.java`, `M/controller/log/QuorumEntry.java`, `M/controller/log/QuorumBatch.java`, `M/controller/log/QuorumEntryCodec.java`, `M/controller/metadata/MetadataImage.java`, `M/controller/metadata/MetadataImageCodec.java`, `M/controller/metadata/MetadataStateMachine.java`; tests `T/controller/ControllerConfigTest.java`, `T/controller/log/QuorumEntryCodecTest.java`, `T/controller/metadata/MetadataStateMachineTest.java`.

**Interfaces:** `QuorumEntryCodec.encode(QuorumEntry):byte[]`, `decode(byte[]):QuorumEntry`; `MetadataImage(long appliedOffset,List<TopicCreated> topics)`; `MetadataStateMachine.apply(QuorumBatch):void`, `restore(MetadataImage):void`, `image():MetadataImage`, `find(String):TopicCreated`; image codec encode/decode analogous. Entry envelope `version:i16=1,kind:u8(LeaderChange=1,Topic=2,ReadBarrier=3),epoch:i64,payload`; Topic payload reuses MetadataEventCodec, LeaderChange payload id:i32, barrier empty.

- [ ] Write codec golden/invalid-version/trailing/overflow tests, immutable image tests and config range tests; state machine only accepts contiguous batches beginning at appliedOffset.
```java
var state = new MetadataStateMachine();
state.apply(new QuorumBatch(0, List.of(new QuorumEntry.LeaderChange(1, 0))));
state.apply(new QuorumBatch(1, List.of(new QuorumEntry.ReadBarrier(1))));
assertEquals(2, state.image().appliedOffset());
assertTrue(state.image().topics().isEmpty());
```
- [ ] Run `mvn -Dtest=ControllerConfigTest,QuorumEntryCodecTest,MetadataStateMachineTest test`; expect missing new classes/tests to fail, not unrelated baseline failure.
- [ ] Implement envelope codec, config builder/validation and image codec (`applied:i64,count:i32, length-prefixed existing metadata events sorted by name`); use fresh TopicCatalog on restore. Kernel:
```java
if (batch.baseOffset() != image().appliedOffset()) throw new IOException("Apply gap");
for (QuorumEntry entry : batch.entries()) {
    if (entry instanceof QuorumEntry.Topic topic) catalog.apply(topic.event());
}
appliedOffset = batch.nextOffset();
```
- [ ] Run same command; assert duplicate same event idempotent, conflicting ID/name rejected, control entries advance apply, batch has exactly one epoch and nonempty entries.
- [ ] Stage task files only; commit `feat: define controller metadata records and configuration`.

### Task 2: Durable publication, identity và hard-state journal

**Files:** Create `M/controller/persistence/DurableFiles.java`, `StateJournal.java`, `QuorumStateStore.java` in same directory; `T/controller/persistence/StateJournalTest.java`, `IdentityFormatTest.java`, `DurableFilesTest.java`; `T/controller/support/FaultFiles.java`; `docs/controller-storage-v1.md`.

**Interfaces:** `DurableFiles.forceFile(Path):void`, `syncDirectory(Path):void`, `writeNew(Path,byte[]):void`, `verifySupport(Path):void`; `StateJournal.open(Path,DurableFiles):StateJournal`, `append(short,byte[]):long`, `frames():List<Frame>`, `close()`; nested `Frame(long sequence,short type,byte[] payload)`. `QuorumStateStore.format(Path,ClusterIdentity,DurableFiles):void`, `open(Path,ClusterIdentity,DurableFiles):QuorumStateStore`, `persistVote(long,int):void`, `epoch():long`, `votedFor():int`, `journal():StateJournal`, `close()`. Store owns root lock. `FaultFiles` records operations and injects IOException at numbered boundary; separate volatile vs forced file bytes and names for simulated power loss.

- [ ] Write vote-reopen, vote-twice-same-epoch, format-existing, wrong identity, malformed final frame and every write/force failure tests.
```java
try (var store = QuorumStateStore.open(root, identity, files)) {
    store.persistVote(7, 1);
    assertThrows(IllegalStateException.class, () -> store.persistVote(7, 2));
}
try (var store = QuorumStateStore.open(root, identity, files)) {
    assertEquals(7, store.epoch());
    assertEquals(1, store.votedFor());
}
```
- [ ] Run `mvn -Dtest=StateJournalTest,IdentityFormatTest,DurableFilesTest test` RED.
- [ ] Implement framing and exact layout above. `persistVote` permits (-1→candidate) once in epoch, repeated identical vote and increased epoch; rejects decreasing epoch or (candidate→-1/other) in same epoch. Publish memory after force:
```java
journal.append(HARD_STATE, encodeHardState(nextEpoch, nextVote));
epoch = nextEpoch;
votedFor = nextVote;
```
- [ ] Run tests GREEN, including root lock contention, full checksum mismatch not tail repair, highest valid complete frame retained, force failure prevents vote response. Write actual filesystem support findings in storage doc; capability probe alone is not power-failure proof.
- [ ] Commit `feat: persist controller identity and voting state` with task files.

### Task 3: Storage primitives cho nonzero base và prefix deletion

**Files:** Modify `M/storage/PartitionLog.java`, `LogRecovery.java`, `LogIo.java`; create `M/storage/LogOpenOptions.java`, `DirectoryDurability.java`; tests `T/storage/PartitionNonzeroBaseTest.java`, `PartitionPrefixTest.java`, `PartitionStrictDurabilityTest.java`; update `docs/storage-format-v1.md`.

**Interfaces:** `LogOpenOptions(long startOffset,long minimumEndOffset,boolean createIfMissing,DirectoryDurability directories)`; `DirectoryDurability.sync(Path):void throws IOException`; new `PartitionLog.open(Path,LogConfig,LogOpenOptions)`, package-private injected LogIo overload retained; `deleteSegmentsBefore(long boundary):long` returns first retained segment base. Caller must journal prefix intent before mutation. To compute intent first expose `prefixStartAfter(long boundary):long`, no mutation. Existing open(Path,LogConfig) delegates options(0,0,true,no-op) to preserve Phase 1/2 contract.

- [ ] Write empty-at-100, missing-data with create=false, read below start, truncate below start, partial tail crossing minimumEnd and prefix crash tests.
```java
var options = new LogOpenOptions(100, 100, true, directorySync);
try (var log = PartitionLog.open(root, config, options)) {
    assertEquals(100, log.logStartOffset());
    assertEquals(100, log.append(records).firstOffset());
    assertThrows(IllegalArgumentException.class, () -> log.read(99, 1));
}
```
- [ ] Run `mvn -Dtest=PartitionNonzeroBaseTest,PartitionPrefixTest,PartitionStrictDurabilityTest test` RED.
- [ ] Change recovery scan initial expected offset to options.startOffset, require first segment exactly there, check recovered end >= minimumEnd before any repair. Propagate directory durability after segment creation/deletion and before durable publication. Kernel:
```java
if (recovered.nextOffset() < options.minimumEndOffset())
    throw new CorruptLogException("Recovery would remove committed prefix");
// only now perform recorded tail repair and force validated files
```
- [ ] Run new tests plus `mvn -Dtest="vn.huyqt.logbroker.storage.*Test" test`; verify golden storage bytes unchanged and no floorKey(null) below retained start. Prefix delete never removes active segment and returns actual retained start.
- [ ] Commit `feat: support nonzero storage origins and safe prefix primitives`.

### Task 4: QuorumLog, epoch index và resumable mutation intents

**Files:** Create `M/controller/log/QuorumLog.java`, `EpochIndex.java`, `M/controller/persistence/GenerationStore.java`; tests `T/controller/log/QuorumLogTest.java`, `EpochIndexTest.java`, `T/controller/persistence/GenerationRecoveryTest.java`; extend storage doc.

**Interfaces:** `GenerationStore.open(QuorumStateStore,DurableFiles,LogConfig):GenerationStore`, `log():QuorumLog`, `generation():UUID`, `checkpointCommit(long):void`, `committedOffset():long`, `close()`. `QuorumLog.append(long,List<QuorumEntry>):QuorumBatch` (first argument epoch), `appendReplica(QuorumBatch):void`, `read(long,int):List<QuorumBatch>`, `flush():long`, `truncate(long,long knownCommit):void`, `end():long`, `durableEnd():long`, `epochs():EpochIndex`; `EpochIndex.positionAt(long):LogPosition`, `endOfEpoch(long):long`, `commonPrefix(long followerEnd,long followerEpoch):LogPosition`. Index copied into loop, not read concurrently while mutating.

- [ ] Write exact batch preservation, wrong base/epoch, append-before-flush, divergent suffix and crash during truncate tests.
```java
QuorumBatch batch = leader.append(1, List.of(new QuorumEntry.ReadBarrier(1)));
follower.appendReplica(batch);
assertEquals(batch, follower.read(batch.baseOffset(), 1024).getFirst());
assertEquals(batch.nextOffset(), follower.flush());
assertThrows(IllegalArgumentException.class,
    () -> follower.truncate(batch.baseOffset(), batch.nextOffset()));
```
- [ ] Run `mvn -Dtest=QuorumLogTest,EpochIndexTest,GenerationRecoveryTest test` RED.
- [ ] Encode entries as LogRecord(timestamp=0,key=null,headers=empty,value=entry bytes), append exactly one storage batch per QuorumBatch. On replica append require base=end and all entries same epoch; account both storage and wire size <= limits. Implement intents with strict replay before regular recovery. For divergence:
```java
long localEpochEnd = epochs.endOfEpoch(followerEpoch);
// If epoch absent, return greatest retained epoch < followerEpoch;
// if its prefix is unavailable, return snapshot directive instead.
long commonEnd = Math.min(followerEnd, localEpochEnd);
```
- [ ] Run tests GREEN; include crash after intent but before deletion, after segment deletion and before truncate, and after operation before DONE; physical suffix removed by valid intent is not mistaken for spontaneous corruption. Any committed inconsistency fails.
- [ ] Commit `feat: adapt storage for epoch-aware quorum replication`.

**Checkpoint 1 — persistence:** Review Tasks 1–4 independently of networking. Verify forced vote survives restart, no committed-tail repair, publication assumptions recorded for actual filesystem. If strict filesystem support is unavailable, resolve deployment/support decision before claiming durable integration readiness; continue independent pure-core work only.

### Task 5: Snapshot file codec và local publication

**Files:** Create `M/controller/snapshot/SnapshotId.java`, `SnapshotStore.java`; tests `T/controller/snapshot/SnapshotStoreTest.java`, `SnapshotPublicationTest.java`; update storage doc.

**Interfaces:** `SnapshotStore(Path,ClusterIdentity,DurableFiles,QuorumStateStore,int maxBytes)`; `create(MetadataImage,long lastEpoch):SnapshotId`, `load(SnapshotId):MetadataImage`, `retained():List<SnapshotId>`, `pin(SnapshotId):Pin`; Pin implements AutoCloseable, has `length():long`, `read(long,int):byte[]`. Publication writes SNAPSHOT_SET, never records an unforced file. `SnapshotId` nested bytes layout matches wire section.

- [ ] Write deterministic roundtrip, cluster mismatch, image/end mismatch, corruption and interrupted publication tests.
```java
SnapshotId id = store.create(new MetadataImage(9, List.of(topic)), 3);
assertEquals(9, store.load(id).appliedOffset());
try (var pin = store.pin(id)) {
    assertTrue(pin.length() > 0);
    assertTrue(pin.read(0, 256).length <= 256);
}
```
- [ ] Run `mvn -Dtest=SnapshotStoreTest,SnapshotPublicationTest test` RED.
- [ ] Implement header `magic:i32=0x51534E31,version:i16,totalLength:i64,clusterId:UUID,voterHash:32,id:SnapshotId,imageLength:i32,image,CRC32C:i32`; CRC all preceding bytes. Exclude nodeId so peer can install. Force temp, rename to unique final name, sync directory, then journal SNAPSHOT_SET. Kernel:
```java
files.forceFile(temporary);
Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
files.syncDirectory(target.getParent());
state.journal().append(SNAPSHOT_SET, encodeRetained(nextRetained));
```
- [ ] Run tests GREEN; duplicate snapshot content ID with different bytes fails, `.partial` never loaded, referenced corrupt snapshot fails, pin prevents unlink; third snapshot publication does not delete coverage needed by older active generation.
- [ ] Commit `feat: publish checksummed metadata snapshots`.

### Task 6: Quorum protocol v1 và bounded codecs

**Files:** Create `M/controller/protocol/QuorumProtocol.java`, `QuorumError.java`, `QuorumCodec.java`; `T/controller/protocol/QuorumCodecTest.java`, `QuorumCodecLimitsTest.java`; `R/controller/protocol-v1-vectors.txt`; `docs/controller-protocol-v1.md`.

**Interfaces:** QuorumProtocol nested `Request`/`Reply` sealed interfaces and records named by wire operation; `Frame(short operation,boolean response,UUID clusterId,int senderId,long requestId,byte[] voterHash,Message message)` with Message parent of Request/Reply; `ReplyMeta(QuorumError error,String message,long epoch,int leaderId)`. Success reply records carry ReplyMeta then table fields; `Failure(ReplyMeta meta)` for errors. `FetchData`, `Divergence`, `SnapshotRequired` sealed FetchPayload; `MetadataView` as defined above. `QuorumCodec.encode(Frame):byte[]`, `decode(byte[],ControllerConfig):Frame`, `preflight(byte[],ControllerConfig):long` returns decoded allocation charge.

- [ ] Write literal golden envelope fixture with empty DescribeQuorum request, independently calculated CRC, every request/reply roundtrip, unknown version/op and array amplification tests. Do not produce golden expected bytes by calling encode under test.
```java
byte[] damaged = codec.encode(frame);
damaged[damaged.length - 1] ^= 1;
assertThrows(IOException.class, () -> codec.decode(damaged, config));
assertEquals(frame.requestId(), codec.decode(codec.encode(frame), config).requestId());
```
- [ ] Run `mvn -Dtest=QuorumCodecTest,QuorumCodecLimitsTest test` RED.
- [ ] Implement exact schemas above; bound arrays before allocation using remaining bytes/minimum element size. Frame CRC does not replace batch/snapshot CRC. Charge conservative `2*frameBytes + 64*decodedElementCount` with checked math before materializing; cap topics/voters/batches and bytes. Kernel:
```java
if (count < 0 || count > maximum || count > input.remaining() / minimumBytes)
    throw new IOException("Invalid array count");
```
- [ ] Run tests GREEN and Phase 2 ProtocolCodecTest suite; verify numbers/bytes unchanged there. Document all layouts and sample hex vectors.
- [ ] Commit `feat: define versioned controller quorum protocol`.

### Task 7: Bounded event/effect runtime và simulation fixture

**Files:** Create `M/controller/consensus/QuorumEvent.java`, `QuorumEffect.java`, `QuorumStatus.java`, `QuorumStateMachine.java`; `M/controller/runtime/ControllerLoop.java`, `OrderedDiskExecutor.java`, `EffectRunner.java`; `T/controller/support/QuorumHarness.java`, `FakeDisk.java`, `SimulatedTransport.java`; `T/controller/runtime/ControllerLoopTest.java`.

**Interfaces:** `QuorumStateMachine.on(QuorumEvent):List<QuorumEffect>`, `status():QuorumStatus`; constructor accepts config and recovered immutable status/index. Events: Tick(nowNanos), PeerRequest(Frame,ReplyRoute), PeerResponse(Frame), DiskDone(token,DiskResult), DiskFailed(token,String), Applied(long end,MetadataImage image), Stop. `QuorumProtocol.ReplyRoute(long connectionId,long requestId)` is opaque routing data, not a network-library object. Nested `DiskToken(long operationId,long epoch,UUID generation)`; disk effects PersistVote, Append(entries), AppendReplica(batch), Flush, Truncate(end), Checkpoint(end), ReadLog(offset,budget), InstallSnapshot(id), each carries token. Other effects Send(peerId,Frame), Reply(ReplyRoute,Frame), Apply(List<QuorumBatch>), Fail(String). Define DiskResult variants VoteSaved, Appended(QuorumBatch,EpochIndex), Flushed(long), Truncated(EpochIndex), Checkpointed(long), Read(List<QuorumBatch>), Installed(UUID generation,SnapshotId,EpochIndex). EffectRunner handles Apply using Task 1 MetadataStateMachine on the loop in bounded batches, then feeds Applied back; this allows Task 9 to test readiness before admin service exists. Extend events with Admin in Task 10.

`ControllerLoop.submit(QuorumEvent,Priority):boolean` with CONTROL/ADMIN/SNAPSHOT; `OrderedDiskExecutor.submit(DiskToken,Callable<DiskResult>):boolean`; `EffectRunner.run(List<QuorumEffect>):void`. Harness `threeNodes(long seed)`, `node(int):QuorumStateMachine`, `tick(Duration)`, `deliverAll()`, `completeDisks()`, `isolate(int)`, `heal()`, `crash(int)`, `restart(int)`, `assertSafety()`. FakeDisk has `failNextForce()`, `completeNext()`, `powerLoss()` and distinct volatile/forced state. Simulation bounds steps to detect livelock.

- [ ] Write completion-reservation, stale generation, control-priority and simulated disk force tests.
```java
var h = QuorumHarness.threeNodes(42);
h.tick(Duration.ofSeconds(4));
// Elections are added in Task 8; this task only checks bounded dispatch.
h.completeDisks();
h.deliverAll();
h.assertSafety();
```
- [ ] Run `mvn -Dtest=ControllerLoopTest test` RED.
- [ ] Implement queues: 256 reserved disk completion slots; 512 control slots; remaining 3,328 shared with admin/snapshot, while pending admin cap stays 1,024. Reserve completion slot before disk submission; release exactly once. Schedule 8 control events then one admitted non-control event for fairness; timer events coalesce. Kernel:
```java
var reservation = completions.tryReserve();
if (reservation == null) return false;
if (!diskQueue.offer(work)) { reservation.close(); return false; }
// Worker publishes completion into its reserved slot, never drops it.
```
- [ ] Run GREEN including delayed old completion resource release, rejection before ownership transfer, disk queue FIFO and fail-close on overflow that violates internal reservation invariants.
- [ ] Commit `feat: add bounded controller runtime and deterministic harness`.

### Task 8: Election, persistent vote và leadership readiness

**Files:** Modify `M/controller/consensus/QuorumStateMachine.java`, events/effects, harness; create `T/controller/consensus/ElectionTest.java`, `ElectionCrashTest.java`.

**Interfaces:** Task 7 transitions and Task 6 Vote/Begin/End bodies; no public new API. RandomGenerator injected into state machine, DeadlineScheduler into loop. Harness adds `elect(int):void` (drive time/network/disks until elected, bounded), `leader():int` (-1 if none). elect does not force state directly.

- [ ] Write no Vote response before force, one vote after restart, stale epoch and log freshness tests.
```java
h.tick(Duration.ofSeconds(4));
assertTrue(h.transport().voteRequests().isEmpty()); // local self-vote not forced
h.completeDisks();
h.deliverAll();
h.assertSafety();
```
Define harness `transport():SimulatedTransport`, `voteRequests():List<Frame>` in this task; collect sent frames as immutable history.
- [ ] Run `mvn -Dtest=ElectionTest,ElectionCrashTest test` RED.
- [ ] Implement vote eligibility `(candidateLastEpoch > localLastEpoch) || (equal && candidateEnd >= localEnd)`. Drain/flush outstanding log mutations before declaring candidate or answering freshness-dependent votes; otherwise complete later with revalidated epoch. Step-down immediately in memory on newer valid epoch, queue persistence, defer epoch-dependent replies until completion. Kernel:
```java
if (request.epoch() < epoch) return staleEpochReply();
// on successful PersistVote completion, recheck current epoch/role/token
// then emit granted response or self-election Vote requests
```
- [ ] Run GREEN: split votes, randomized retry, higher epoch while vote force pending, duplicate Begin from same leader, conflicting same-epoch leaders rejected, restart self-vote, candidate with longer but older-epoch suffix rejected. Winner appends LeaderChange but ready remains false until Task 9 commit/apply.
- [ ] Commit `feat: elect controller leaders with durable votes`.

### Task 9: Pull replication, divergence và durable commit

**Files:** Create `M/controller/consensus/ReplicationTracker.java`; modify state machine/effect runner/harness; `T/controller/consensus/ReplicationTest.java`, `CommitRuleTest.java`, `DivergenceTest.java`.

**Interfaces:** `ReplicationTracker.reset(long epoch)`, `confirm(int voter,long end,long lastEpoch,EpochIndex index)`, `committable(long localDurable,long epoch,EpochIndex index):long`, `matches():Map<Integer,Long>`; follower progress from durable prefix only. Harness adds `pauseDisk(int)`, `resumeDisk(int)`, `appendBarrier(int):void` through production proposal event, `settle():void` bounded until no immediately runnable work (not arbitrary sleeps).

- [ ] Write append-majority without force, inherited old-epoch log, exact-boundary reconciliation and follower commit clamp tests.
```java
h.elect(0);
long before = h.node(0).status().commit();
h.pauseDisk(1); h.pauseDisk(2);
h.appendBarrier(0); h.settle();
assertEquals(before, h.node(0).status().commit());
h.resumeDisk(1); h.settle();
assertTrue(h.node(0).status().commit() > before);
```
- [ ] Run `mvn -Dtest=ReplicationTest,CommitRuleTest,DivergenceTest test` RED.
- [ ] Implement one Fetch in flight/follower, epoch-index validation, fixed deadline idle wait, data read via disk effect, append/force completion before next durable Fetch. Commit kernel:
```java
long candidate = Math.min(localDurable, majorityMatchingEnd);
if (candidate > commit && epochs.positionAt(candidate).lastEpoch() == epoch)
    advanceCommit(candidate);
```
Guard majorityMatchingEnd batch boundary; dispatch immutable committed batches to apply; readiness only after own LeaderChange applied. Checkpoint after data forced. Old completion updates physical facts only if relevant, never stale acknowledgments.
- [ ] Run GREEN: leader isolation/heal, min(durable,leaderCommit), missing epochs, offsets beyond leader end, snapshot directive when prefix unavailable, current-epoch marker, local force failure, stale Fetch after truncate, duplicate RPC idempotence, challenge expiry and replay not extending leader liveness.
- [ ] Commit `feat: replicate metadata and commit durable quorum prefixes`.

**Checkpoint 2 — consensus:** Review Tasks 6–9 traces for same-epoch vote, leader completeness, commit rule, durable match and liveness challenge. Run all controller unit tests plus storage tests. Do not treat a three-node happy-path election as proof of safety.

### Task 10: CreateTopic admission và linearizable reads

**Files:** Create `M/controller/ControllerService.java`; modify state machine/events/effects, effect runner and harness; `T/controller/ControllerServiceTest.java`, `LinearizableReadTest.java`.

**Interfaces:** `ControllerService.createTopic(String,int,long deadlineNanos):CompletableFuture<UUID>`, `readMetadata(long deadlineNanos):CompletableFuture<QuorumProtocol.MetadataView>`, `readLocalMetadata():CompletableFuture<MetadataView>`, `describe():CompletableFuture<QuorumStatus>`. Service sends `QuorumEvent.Admin(long invocationId,Request request,long deadlineNanos)`; state machine emits `QuorumEffect.CompleteAdmin(long invocationId,Reply reply)` and consumes the existing Applied event to release waiters. Reuse Task 7 EffectRunner apply path. Harness adds `service(int):ControllerService`; test deadlines use injected monotonic time.

- [ ] Write duplicate pending CreateTopic, conflict, UUID preservation through response loss, barrier grouping and request-after-barrier tests.
```java
var first = h.service(0).createTopic("orders", 3, deadline);
var duplicate = h.service(0).createTopic("orders", 3, deadline);
h.settle();
assertEquals(first.join(), duplicate.join());
var read = h.service(0).readMetadata(deadline);
assertFalse(read.isDone());
h.settle();
assertEquals(LINEARIZABLE, read.join().consistency());
```
- [ ] Run `mvn -Dtest=ControllerServiceTest,LinearizableReadTest test` RED.
- [ ] Implement pending name→(UUID,partitions,offset,waiters), pending capacity, coalesced proposal batches and read cohorts. Existing-topic successful CreateTopic must also prove current leadership: route response through a fresh read barrier before returning the UUID; duplicate pending request already waits its own committed proposal. Kernel:
```java
List<Long> cohort = List.copyOf(waitingReads);
waitingReads.clear();
// Store cohort under the newly appended barrier's nextOffset.
// A request admitted after this point waits for a different barrier.
```
Apply committed batches before completing futures; no user callback runs inline on consensus loop (complete through bounded response dispatcher with reserved slots). Snapshot/local read returns immutable image. Complete pending waiters on epoch loss with retryable error; keep log/pending reservation facts until log reconciles, never refund topic capacity merely because client timed out.
- [ ] Run GREEN: leader not ready, timeout followed by later commit, pending limit=1, isolation blocks read, local metadata label/offsets, late response on old epoch, new leader replay before conflicting create. Verify reads complete from image at or after barrier apply, never speculative image.
- [ ] Commit `feat: expose idempotent topic commands and linearizable reads`.

### Task 11: Local snapshots và retention coordination

**Files:** Create `M/controller/snapshot/SnapshotCoordinator.java`; modify GenerationStore, SnapshotStore, loop/effects; `T/controller/snapshot/SnapshotCoordinatorTest.java`, `SnapshotRetentionTest.java`.

**Interfaces:** `SnapshotCoordinator.onApplied(MetadataImage,long lastEpoch,long appendedBytes):void`, `onCreated(SnapshotId):void`, `close():void`; emits snapshot disk operations through OrderedDiskExecutor. `GenerationStore.retainPrefix(long olderSnapshotEnd):void` persists prefix intent and invokes Task 3 primitives; `SnapshotStore.releaseObsolete():void` removes only unreferenced snapshots after checking active generation and pins.

- [ ] Write trigger measured since previous snapshot, one in-flight creation, image-at-S despite new apply, and two-snapshot coverage tests.
```java
coordinator.onApplied(imageAt10, 2, thresholdBytes);
coordinator.onApplied(imageAt11, 2, thresholdBytes + 100);
assertEquals(1, disk.pendingSnapshots());
disk.completeNext();
assertEquals(10, snapshots.retained().getFirst().endOffset());
```
Add `FakeDisk.pendingSnapshots():int` in this task; fixture initializes coordinator/disk wiring explicitly.
- [ ] Run `mvn -Dtest=SnapshotCoordinatorTest,SnapshotRetentionTest test` RED.
- [ ] Serialize immutable bounded image off-loop in at most 256 KiB slices scheduled as low-priority disk tasks; control I/O may run between slices, but no log mutation may overtake an earlier dependent mutation. Account retained image and chunk buffers, release them on completion/failure. Publish snapshot, then evaluate two retained IDs and actual segment boundaries; retained list newest first. Kernel:
```java
if (retained.size() == 2) {
    long covered = retained.get(1).endOffset();
    generations.retainPrefix(covered);
}
```
GenerationStore durable prefix intent references current snapshot set; never delete last active segment. Snapshot files needed by the currently published generation's starting snapshot remain referenced even if not in newest two until generation recovery base is advanced safely: publish updated GENERATION referencing a newer retained snapshot while keeping same log generation/start and nondecreasing commit, before removing the old reference. Recovery replays from that snapshot and retained suffix.
- [ ] Run GREEN with crash after each journal/force/delete boundary, pinned snapshot, failed publication, all-control-record workload, retention while apply advances, and older snapshot + suffix reconstructing same latest image.
- [ ] Commit `feat: snapshot committed metadata and retain recoverable prefixes`.

### Task 12: Snapshot transfer và atomic generation install

**Files:** Create `M/controller/snapshot/SnapshotTransfer.java`; extend SnapshotStore/GenerationStore, events/effects/results; tests `T/controller/snapshot/SnapshotTransferTest.java`, `SnapshotInstallCrashTest.java`.

**Interfaces:** `SnapshotTransfer.begin(SnapshotId,long epoch):void`, `accept(FetchSnapshotReply):void`, `cancel():void`; `SnapshotStore.beginDownload(SnapshotId):void`, `writeChunk(SnapshotId,long position,byte[] bytes):void`, `finishDownload(SnapshotId,long totalLength):MetadataImage`; `GenerationStore.install(SnapshotId,MetadataImage):void`, result Installed contains new generation UUID as well as EpochIndex. SnapshotTransfer feeds requests through Send and receives correlated responses; file calls go through disk effects.

- [ ] Write repeated chunk, out-of-order position, changed snapshot ID, invalid checksum/total length, restart partial, and install crash cases.
```java
transfer.begin(id, 7);
transfer.accept(firstChunk);
transfer.accept(firstChunk); // exact duplicate acknowledged, no double append
assertEquals(firstChunk.chunk().length, disk.downloadedBytes());
transfer.accept(corruptNextChunk);
assertFalse(disk.hasPublishedGenerationFor(id));
```
Define FakeDisk `downloadedBytes():long`, `hasPublishedGenerationFor(SnapshotId):boolean`. Duplicate accepted only when position/data checksum match saved chunk; otherwise discard transfer.
- [ ] Run `mvn -Dtest=SnapshotTransferTest,SnapshotInstallCrashTest test` RED.
- [ ] Stream chunks to bounded temp file, verify whole snapshot checksum and identity before installation. Install disk task revalidates committed/applied boundary and current transfer token immediately before publication; no concurrent apply/mutation while switching generation. Kernel:
```java
if (id.endOffset() <= applied || id.endOffset() < committed)
    throw new IOException("Snapshot does not advance safe prefix");
// force snapshot + new empty log at S + directory entries first
journal.append(GENERATION, encodeGeneration(newGeneration, id.endOffset(), id));
```
Discard old speculative suffix, restore image S only after journal force, preserve vote/epoch. Epoch change during download cancels transfer; completion of already published install updates physical generation/image facts but emits no stale ACK. Snapshot prior to current applied or contradictory known committed epoch must be rejected, not used to repair divergence silently.
- [ ] Run GREEN with leader switch, SNAPSHOT_NOT_FOUND restart, max one download/two upload, pinned file lifetime, old generation GC failure safe, and power-loss model at every publication boundary. Restart must select old or new complete generation and never mixed files.
- [ ] Commit `feat: transfer snapshots and install controller log generations`.

**Checkpoint 3 — service/snapshot:** Review Tasks 10–12 for linearization point, timeout uncertainty, two-snapshot coverage, persistent references and generation publication. Run deterministic isolated leader + follower snapshot catch-up scenario before TCP integration.

### Task 13: Netty quorum transport với resource accounting

**Files:** Create `M/controller/transport/QuorumTransport.java`, `NettyQuorumTransport.java`; modify `M/transport/netty/BoundedFrameDecoder.java` only for generic length overload; `T/controller/transport/QuorumTransportTest.java`, `QuorumTransportBudgetTest.java`; extend existing decoder tests.

**Interfaces:** `QuorumTransport.start(InetSocketAddress,Consumer<Inbound>,Consumer<Throwable>):InetSocketAddress`, `send(int peerId,Frame):CompletableFuture<Void>`, `reply(ReplyRoute,Frame):CompletableFuture<Void>`, `stopAccepting():void`, `closeAsync():CompletableFuture<Void>`. Nested `Inbound(ReplyRoute route,Frame frame)` references Task 7 routing data. Implementation tracks request connection internally; route includes connection incarnation and requestId, never requestId alone. Runtime creates PeerRequest(frame,route); consensus produces Reply(route,frame). Admin requests on same listener are routed through ControllerService preserving ReplyRoute. Admin outbound client uses separate connect/send adapter in Task 15.

- [ ] Write split/coalesced frame, identity mismatch without epoch change, out-of-order correlation, disconnect cleanup and overload tests with Netty EmbeddedChannel plus loopback.
```java
long before = inboundBudget.used();
channel.writeInbound(Unpooled.wrappedBuffer(partialFrame));
channel.close();
assertEquals(before, inboundBudget.used());
```
- [ ] Run `mvn -Dtest=QuorumTransportTest,QuorumTransportBudgetTest test` RED.
- [ ] Add `BoundedFrameDecoder(int minFrameBytes,int maxFrameBytes,ResourceBudget,DeadlineScheduler)`; old constructors retain 12-byte Phase 2 minimum. Quorum uses min69 and max8MiB. Decode on bounded two-worker validation pool (256 tasks), serialize validation per connection. Reserve raw bytes before allocation, decoded charge before decode, release when all core/disk consumers finish. Reserve outbound until write future completes. Kernel:
```java
var lease = outbound.reserve(encoded.length).orElseThrow(OverloadedException::new);
channel.writeAndFlush(buffer).addListener(result -> lease.close());
```
Use a defined private `OverloadedException extends RuntimeException` in adapter or explicit failedFuture instead; do not leak it into public protocol. Allocate inbound/outbound budgets as 8 MiB reserved peer-control each plus 56 MiB shared each. Separate peer/admin connections, 64 total connections, 32 in-flight/connection, partial frame deadline 30 s. Reserve peer control queues; snapshot transport never exhausts vote/Fetch response capacity. Close invalid frames safely; no response amplification for untrusted oversized requests.
- [ ] Run GREEN and all existing transport tests; enforce unrecognized peer/cluster before delivering event, failure callback associated with correct connection, late frame dropped after incarnation closed, shutdown frees budget and threads. Netty pipeline retains no ByteBuf across core boundary.
- [ ] Commit `feat: connect controller peers through bounded Netty transport`.

### Task 14: Controller composition, startup, failure và shutdown

**Files:** Create `M/controller/ControllerNode.java`; modify EffectRunner and GenerationStore; `T/controller/ControllerLifecycleTest.java`, `ControllerRecoveryTest.java`.

**Interfaces:** `ControllerNode.open(Path,ControllerConfig,DurableFiles):ControllerNode`, `start():InetSocketAddress`, `service():ControllerService`, `status():CompletableFuture<QuorumStatus>`, `close():void`. open recovers without binding; start binds after recovery. GenerationStore exposes `recoveredImage():MetadataImage`, `epochIndex():EpochIndex` and pending intent replay from earlier tasks. Root lock owner remains QuorumStateStore until all worker file use stops.

- [ ] Write bind-after-replay, commit checkpoint lag, corrupt checkpoint, missing files, metadata application failure and blocked disk shutdown tests.
```java
try (var node = ControllerNode.open(root, config, files)) {
    assertFalse(node.status().join().ready());
    node.start();
    assertEquals(UNATTACHED, node.status().join().role());
}
```
- [ ] Run `mvn -Dtest=ControllerLifecycleTest,ControllerRecoveryTest test` RED.
- [ ] Wire recovery: identity→journal→intents→generation/snapshot→strict log→epoch index→committed image→runtime→listener. If log max epoch exceeds hard state, persist higher epoch with no vote before listening, never lower existing state. Replay only up to max(snapshot end, valid COMMIT), then await consensus for suffix. Kernel:
```java
if (checkpoint > log.durableEnd()) throw new IOException("Commit exceeds durable coverage");
for (QuorumBatch batch : committedBatches) metadata.apply(batch);
```
Fatal I/O stops protocol participation and fails waiters; status endpoint may remain with immutable last valid view. Shutdown stops admission, drains already accepted disk work, closes channels/workers/files in dependency order. If deadline expires with disk active, close reports timeout and retains lock/storage until worker terminates or process exits; no lock release in unconditional finally.
- [ ] Run GREEN: two roots same node detection by config/operator contract documented (root lock cannot prevent duplicate ID on different disks), concurrent open same directory fails, failed node gives NODE_UNAVAILABLE, no speculative local catalog after restart, completion callback never blocks event loop.
- [ ] Commit `feat: compose recoverable controller lifecycle`.

### Task 15: Admin client, bootstrap và retry uncertainty

**Files:** Create `M/controller/client/ControllerClient.java`, `ControllerClientException.java`, `ControllerClientTransport.java`, `NettyControllerClientTransport.java`; tests `T/controller/client/ControllerClientTest.java`, `ControllerClientRetryTest.java`.

**Interfaces:** `ControllerClient(ClusterIdentity,List<InetSocketAddress>,DeadlineScheduler,ControllerClientTransport.Factory)`; `createTopic(String,int):CompletableFuture<UUID>`, `metadata():CompletableFuture<MetadataView>`, `localMetadata(int nodeId):CompletableFuture<MetadataView>`, `describe(int nodeId):CompletableFuture<QuorumStatus>`, `close()`. Client transport `connect(address,Consumer<Frame>,Consumer<Throwable>):CompletableFuture<Void>`, `send(Frame):CompletableFuture<Void>`, `close()`; factory `create():ControllerClientTransport`. Exception carries QuorumError and Outcome {NOT_SENT,UNKNOWN}. Reuse codec and frame budget logic from Task 13; no BrokerClient protocol reuse.

- [ ] Write lost CreateTopic response then NOT_LEADER then deadline, wrong cluster bootstrap, stale requestId and barrier retry tests.
```java
var result = client.createTopic("orders", 3);
fake.acceptThenLoseResponse();
fake.replyNotLeader();
clock.advance(Duration.ofSeconds(31)); clock.runDue();
var error = assertInstanceOf(ControllerClientException.class,
    assertThrows(CompletionException.class, result::join).getCause());
assertEquals(UNKNOWN, error.outcome());
```
Create package-private `FakeClientTransport` inside test with explicit acceptThenLoseResponse/replyNotLeader helpers driving real Frames.
- [ ] Run `mvn -Dtest=ControllerClientTest,ControllerClientRetryTest test` RED.
- [ ] Implement per-attempt connection/request IDs, shared absolute deadline, 50 ms exponential backoff capped at 1 s with seeded jitter. Bootstrap list cycles when hints unavailable; accept hints only for configured voters. Kernel:
```java
everPossiblySent |= attemptReachedTransport;
// A later redirect cannot change this back to false.
Outcome outcome = everPossiblySent ? UNKNOWN : NOT_SENT;
```
Transport enqueue is conservative may-have-sent point; final definitive success resolves uncertainty. Retry CreateTopic same name/count only, not all future mutations by default. Limit 32 in-flight per connection and 1,024 active client requests, with bounded bytes and one deadline timer/request.
- [ ] Run GREEN with disconnect before enqueue, overloaded bootstrap, success on duplicate same UUID, timeout cancellation removes correlation, close completes all pending futures and releases budgets. Metadata default always linearizable; local calls do not silently route to another node.
- [ ] Commit `feat: add controller admin client with safe retry semantics`.

### Task 16: CLI, configuration và three-node example

**Files:** Create `M/controller/ControllerMain.java`, `M/controller/client/ControllerCli.java`; `T/controller/ControllerCliTest.java`; `config/controller-0.properties`, `controller-1.properties`, `controller-2.properties`; `docs/controller-configuration.md`; modify README.

**Interfaces:** mains call testable `run(String[],PrintStream,PrintStream):int`; ControllerMain run blocks until shutdown, interruption triggers close. CLI commands below with explicit `--cluster`, `--voters`, `--node` where relevant; format command invokes QuorumStateStore.format and no network.

- [ ] Write argument validation, format refusal existing directory, create output UUID and local-read label tests.
```java
int code = ControllerCli.run(new String[]{"format", "--data", existing.toString()}, out, err);
assertNotEquals(0, code);
assertArrayEquals(originalBytes, Files.readAllBytes(existing.resolve("identity.bin")));
```
- [ ] Run `mvn -Dtest=ControllerCliTest test` RED.
- [ ] Implement options parser without new framework, reject unknown/duplicate keys, print actionable errors without stack traces unless debug. Write config docs listing every spec cap and added transport/journal caps, filesystem support and log directory isolation. Kernel:
```java
return switch (command) {
    case "generate-cluster-id" -> printUuid(out);
    case "format" -> format(options, out);
    case "create-topic", "metadata", "local-metadata", "describe-quorum" -> admin(command, options, out);
    default -> usage(err);
};
```
Define the four private helpers in ControllerCli with these exact signatures inferred from parsed `Map<String,String>` options, each returning int; main exits with returned status.
- [ ] Run GREEN and prepare smoke commands (do not reuse existing user data):
```powershell
mvn verify dependency:copy-dependencies
$clusterId = (java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.controller.client.ControllerCli generate-cluster-id).Trim()
$voterList = '0@127.0.0.1:19090,1@127.0.0.1:19091,2@127.0.0.1:19092'
# Copy $clusterId into cluster.id in all three config files.
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.controller.client.ControllerCli format --data target/controller-0 --node 0 --cluster $clusterId --voters $voterList
# Repeat format with controller-1/node 1 and controller-2/node 2.
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.controller.ControllerMain --config config/controller-0.properties
# Run node 1 and node 2 in separate terminals.
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.controller.client.ControllerCli create-topic --cluster $clusterId --voters $voterList --name orders --partitions 3
```
Checked-in sample config uses one explicit documented example UUID shared across nodes; docs explain replacing cluster.id with the generated UUID before starting. Include complete commands for all three format/start steps, `metadata`, `local-metadata --node 1`, `describe-quorum --node 0` examples and expected consistency/offset fields.
- [ ] Commit `docs: add runnable metadata quorum and administration examples`.

**Checkpoint 4 — operational:** Tasks 13–16 must demonstrate three controllers with real TCP, create/read/restart and bounded resource cleanup. Record actual filesystem/runtime used; inability to validate strict persistence is a remaining acceptance gap, not a skipped passing test.

### Task 17: Deterministic fault campaigns và model invariants

**Files:** Create `T/controller/consensus/QuorumFaultCampaignTest.java`, `LinearizabilityHistoryTest.java`; extend support fixtures; `R/controller/fault-seeds.txt`.

**Interfaces:** Harness adds `runSchedule(long seed,int steps):void`, `history():List<HistoryEvent>`, `assertAcknowledgedTopicsSurvive():void`. HistoryEvent sealed Invocation(id,command,start), Completion(id,result,end), Crash(node), Delivery(source,target,requestId); test-only `LinearizabilityHistoryTest.check(List<HistoryEvent>):boolean` enumerates serial orders respecting real-time edges for bounded histories <=8 completed admin operations, model state name→(id,partitions). Pending timed-out operations may either take effect or not; local reads excluded from linearizable history.

- [ ] Write tests first against intentionally faulty fixture variants (ack-before-force, forget-vote-on-restart, old barrier reuse) to demonstrate that invariants/history checker detect them; variants remain test-only.
```java
for (long seed : List.of(1L, 7L, 42L, 20260928L)) {
    var h = QuorumHarness.threeNodes(seed);
    h.runSchedule(seed, 2_000);
    h.assertSafety();
    h.heal(); h.tick(Duration.ofSeconds(10)); h.settle();
    h.assertAcknowledgedTopicsSurvive();
}
```
- [ ] Run `mvn -Dtest=QuorumFaultCampaignTest,LinearizabilityHistoryTest test` RED for missing checker/coverage; deliberately faulty variants must be asserted to fail, never accepted as production expected failure.
- [ ] Implement schedule generator with bounded drops/reorders/disk delays/crashes/snapshot triggers, and eventual healthy suffix long enough for liveness. Oracle tracks committed content per offset across generations and acknowledged topics independently of node implementation. Kernel:
```java
for (var item : previouslyCommitted.entrySet())
    assertEquals(item.getValue(), observedCommitted.get(item.getKey()));
```
Store oracle before truncation/compaction so deleted physical records do not erase evidence. Validate linearizability by invoking abstract CreateTopic/read on each candidate ordering and compare results, not by sorting on internal commit offsets.
- [ ] Run GREEN with reproducible seed/event trace on failure, snapshot install mid-election, log rollover near force, journal/prefix crash and minority long partition. Cap runtime; optional larger seed count via `-Dquorum.seedCount=100` uses same assertions.
- [ ] Commit `test: exercise quorum safety under deterministic faults`.

### Task 18: Multi-process fault tests và final acceptance

**Files:** Create `T/controller/support/ThreeControllerProcesses.java`, `ControllerFaultProxy.java`; `T/integration/ControllerClusterTest.java`, `ControllerCrashTest.java`, `ControllerSnapshotCatchupTest.java`; `docs/controller-verification.md`; update README, spec status only after actual completion.

**Interfaces:** fixture implements AutoCloseable, `startAll():void`, `kill(int):void`, `restart(int):void`, `isolate(int):void`, `heal():void`, `awaitLeader(Duration):int`, `client():ControllerClient`, `awaitConvergence(Duration):void`. Allocate ports dynamically, unique temp roots, capture per-node stdout/stderr, kill only owned processes, preserve logs on failure. Proxy decodes enough envelope to route/drop; it is test-only and does not alter messages silently.

- [ ] Write happy path, leader kill after acknowledged create, restart-all, minority reads and follower snapshot catch-up tests.
```java
try (var cluster = new ThreeControllerProcesses(tempRoot)) {
    cluster.startAll();
    int leader = cluster.awaitLeader(Duration.ofSeconds(20));
    UUID id = cluster.client().createTopic("orders", 3).join();
    cluster.kill(leader);
    cluster.awaitLeader(Duration.ofSeconds(20));
    assertEquals(id, cluster.client().createTopic("orders", 3).join());
    cluster.restart(leader);
    cluster.awaitConvergence(Duration.ofSeconds(20));
}
```
- [ ] Run `mvn -Dtest=ControllerClusterTest,ControllerCrashTest,ControllerSnapshotCatchupTest test` RED. Missing strict filesystem support is environment-blocked acceptance, not a test skip counted green.
- [ ] Implement process fixture and test coordination using protocol readiness/status with bounded polling, not fixed sleeps. Configure small snapshot threshold/segments for catch-up; assert at least one prefix deletion and snapshot install occurred, then UUID/catalog equality. Preserve default production settings outside tests. Kernel:
```java
process.destroyForcibly();
assertTrue(process.waitFor(10, TimeUnit.SECONDS));
// Do not call graceful close before this crash scenario.
```
- [ ] Run targeted tests GREEN, then `mvn clean verify` once for all Phase 1/2/3 tests and CLI smoke commands from docs. Record runtime, filesystem assumptions, seed set, test counts, process crash versus simulated power loss, and remaining limitations. Repeat only tests affected by subsequent fixes.
- [ ] Update README to Phase 3 implemented only when acceptance passes. Commit `test: verify controller quorum recovery across process failures`.

## Coverage và review gates

| Spec section | Tasks | Evidence |
|---|---|---|
| 1–3 scope/architecture | 1,7,14 | Standalone controller, pure core, bounded ownership |
| 4 bootstrap | 2,14,16 | Explicit format/identity and lock tests |
| 5 log/storage | 3,4 | Golden compatibility, nonzero offsets, mutation crash tests |
| 6 election | 8,17 | Durable voting and stale epoch schedules |
| 7 replication/commit | 4,9,17 | Matching durable prefixes/current-epoch rule |
| 8 commands/reads | 10,15,17 | Duplicate/retry and independent history checker |
| 9 protocol/client | 6,13,15 | Literal vectors, resource budgets, uncertainty |
| 10 snapshot | 5,11,12,18 | Publication/install crash matrix and catch-up |
| 11 lifecycle | 2,4,14,18 | Strict restart, fatal disk failure, drain ownership |
| 12 limits | 1,6,7,13,15 | Checked allocation/admission and queue tests |
| 13 CLI | 16 | Three-node documented smoke |
| 14 acceptance | 17,18 | Deterministic faults + multi-process verification |

Final review must explicitly audit: false-positive commit when only append completed; double vote after restart; forgotten old pending topic; read barrier admitted too early; rollback of hard state during snapshot; missing log generation publication force; prefix deletion making older snapshot unusable; disk completion lost on queue pressure; ambiguous request reported NOT_SENT; native filesystem assumptions untested. Review the spec and implementation, not only this checklist.

## Execution handoff

Plan được viết và rà soát ở mức tài liệu; chưa compile snippets, chạy baseline hoặc thực thi task trong lượt lập kế hoạch. Thực thi tuần tự các dependency, review tại bốn checkpoint và final acceptance. Chọn subagent-driven task-by-task hoặc inline execution; không bắt đầu implementation chỉ vì plan đã tồn tại.

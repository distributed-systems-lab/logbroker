# Cluster và quản lý partition — Phase 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chuyển broker/client sang cluster RF=1 dùng metadata quorum, tiếp tục phục vụ partition hiện có khi chỉ mất liên lạc controller, với registration/fencing, metadata observer, recovery và routing được kiểm chứng.

**Architecture:** Controller quyết định metadata bằng committed batches; broker là observer, duy trì image bất biến và mở đúng partition được giao. Lifecycle không có serving lease; disk I/O có thứ tự và completion mang session/generation. ClusterClient giữ các BrokerClient một kết nối, route theo metadata và không retry Produce UNKNOWN.

**Tech Stack:** Java 21, Maven 3.9.x, JUnit Jupiter 5.11.4, Netty 4.2.18.Final hiện có; single module, package `vn.huyqt.logbroker`. Không thêm thư viện Kafka/Raft hoặc framework DI.

**Spec:** [Phase 4 design, bản sửa 2026-10-03](../specs/2026-10-02-cluster-phase-4-design.md). Yêu cầu lập plan ngày 2026-10-03 là bước chuyển từ review spec sang lập kế hoạch; plan không thay đổi các bảo đảm đã chốt.

## Global Constraints

- Chuyển hoàn toàn sang cluster mode; không duy trì broker standalone như một chế độ runtime.
- Dùng cluster và thư mục dữ liệu mới; không migration dữ liệu Phase 2. Không cam kết mở trực tiếp metadata storage Phase 3 bằng format mở rộng của Phase 4.
- Broker đã RUNNING tiếp tục Produce/Fetch khi chỉ mất liên lạc controller/quorum; startup/restart không tự ready từ cache cũ.
- Registration/fence/unfence/assignment phải commit/apply trước khi báo thay đổi thành công. Heartbeat thường không append log; ReadBarrier vẫn dùng cho API đọc linearizable.
- Controllers / quorum: 3 / 2, membership cố định. Broker count tối đa: 32; demo dùng 3. RF: Chỉ nhận 1.
- Topics / tổng partitions: 128 / 1.024; capacity có tính pending. Các số này là defaults/validation, không phải kích thước mảng cố định trong format.
- Heartbeat interval / RPC deadline: 1 s / 2 s. Controller session timeout: 10 s; leader mới dùng cửa sổ quan sát 10 s.
- Client operation / shutdown deadline: 30 s / 30 s. Observer fetch idle wait / data budget: 100 ms / 4 MiB.
- Metadata batch / internal frame tối đa: 1 MiB / 8 MiB. Snapshot chunk / envelope tối đa: 256 KiB / 64 MiB.
- Observer fetch / download mỗi broker: Tối đa 1 / 1. Client broker connections: Tối đa 32; lazy connect.
- Advertised host: tối đa 255 UTF-8 bytes; port 1–65535. Broker IDs và voter IDs có namespace theo role.
- Offset exclusive, apply/snapshot/checkpoint tại batch boundary. TopicRecord + N PartitionRecord là một atomic batch.
- Observer không vote, không tính quorum majority; chỉ nhận committed batches.
- highWatermark = logEndOffset trong RF=1 tại cùng read snapshot; không đồng nghĩa flushed. Giữ APPENDED/FLUSHED.
- Không tự retry Produce UNKNOWN. Không xóa/tạo lại log rỗng khi dữ liệu đã provision bị mất.
- Strict durability/process acceptance chạy trên Linux/WSL ext4 đã xác minh directory durability; Windows core tests không chứng minh điều này.
- Không disk I/O/chờ future trên Netty hoặc controller event loop; bounded queues và reserve completion capacity trước dispatch I/O.
- Ngoài phạm vi: ISR management, replication/failover, reassignment, migration/rolling upgrade, dynamic feature levels, delete/unregister runtime API, combined mode và controlled shutdown chuyển leader.

---

## 0. Cách dùng và khảo sát baseline

Baseline được đọc tại `9131f19`. Root: `D:/User2/distributed-system-labs/java-log-broker`. Kiểm tra HEAD/status lại khi execution bắt đầu; không ghi đè sửa đổi của người dùng. Chỉ tạo isolated worktree khi thực thi, không cần cho lượt viết plan. Đây là một plan vì controller, observer và client cùng hiện thực một contract cluster; checkpoint có thể review/test riêng nhưng không phải các sản phẩm độc lập.

- [ ] Trước Task 1, ghi `git rev-parse HEAD`, `java -version`, `mvn -version`, platform/filesystem và chạy `mvn clean verify`. Maven/dependency/environment failure không phải RED regression hợp lệ. Ghi counts/skips, không sao chép số 262 lịch sử làm bằng chứng mới.
- [ ] Đọc `AGENTS.md`, spec và `docs/controller-verification.md`. Mỗi case liệt kê trong task cần một test riêng với vòng RED → thay đổi nhỏ → GREEN. Không viết cả subsystem trước lần test đầu.
- [ ] Snippets là lõi test/thuật toán; bổ sung package/imports và cleanup thật. Không đổi assertion thành mock trả sẵn đáp án. Methods disk đều `throws IOException`; chỉ gọi trên ordered worker.
- [ ] Mỗi task kết thúc bằng stage đúng files trong Files block, `git diff --cached --check` và commit message đã ghi. Không `git add .`.
- [ ] Không sửa storage record-batch format Phase 1. Preserve v1 codec fixtures; runtime cluster dùng version mới. Test legacy codec/core không đồng nghĩa duy trì standalone runtime.

Đã xác minh: MetadataStateMachine.apply công bố image sau batch; QuorumStateMachine có check-quorum, DrainProposals và Applied completion. SnapshotStore chỉ dùng QuorumStateStore cho SNAPSHOT_SET journal nhưng constructor gắn voter state. MetadataImageCodec hard-code128 descriptors; snapshot sizing v1 phải thay. RequestDispatcher nhận concrete MetadataService; Producer/Consumer nhận BrokerClient. ClientTransport mang Protocol frames: mở rộng DTO/codec theo version, không tạo raw-byte API trùng lặp.

## 1. Bản đồ file và phụ thuộc

Prefix mở rộng chính xác: `M/` = `src/main/java/vn/huyqt/logbroker/`, `T/` = `src/test/java/vn/huyqt/logbroker/`, `R/` = `src/test/resources/`. File paths trong tasks dùng các prefix này.

| Task | File mới chính | Boundary |
|---|---|---|
| 1 | M/controller/metadata/ClusterRecords.java, MetadataLimits.java | Record schema và limits |
| 2 | T/controller/metadata/ClusterMetadataApplyTest.java | Atomic apply + image v2 |
| 3 | M/controller/protocol/BrokerControlProtocol.java | Control DTOs/opcodes/role |
| 4 | M/controller/metadata/ClusterControlManager.java | Leader-only sessions/assignment |
| 5 | M/controller/metadata/HeartbeatTracker.java | Volatile liveness/recovery target |
| 6 | M/controller/metadata/ObserverReadService.java | Committed fetch và upload budgets |
| 7 | M/broker/cluster/BrokerIdentityStore.java, BrokerClusterConfig.java | Format/identity/config |
| 8 | M/broker/cluster/ObserverStore.java; M/controller/snapshot/SnapshotJournal.java | Observer persistence không voter state |
| 9 | M/broker/cluster/BrokerControlClient.java, MetadataObserver.java | Controller connection + pull loop |
| 10 | M/broker/cluster/BrokerLifecycle.java, ServingGate.java | Lifecycle và admission |
| 11 | M/broker/cluster/PartitionInventory.java, ClusterPartitionManager.java | Crash-safe provisioning |
| 12 | M/broker/metadata/BrokerMetadata.java, ClusterMetadataService.java | Composition/forwarding |
| 13 | M/protocol/ClusterProtocol.java | Data v2 và guarded I/O |
| 14 | M/client/ClusterClient.java, ClusterClientConfig.java, RequestClient.java | Routing/retry |
| 15 | T/integration/support/Phase4Processes.java | Process acceptance/faults |
| 16 | config/cluster/*.properties; docs/cluster-configuration.md | Demo/docs/status/full verify |

Thứ tự: 1→2→3→4→5→6; 7 dùng1/3; 8 dùng2/7; 9 dùng3/6/8; 10 dùng5/9; 11 dùng7/10; 12 dùng4/9/11; 13 dùng10/12; 14 dùng13; 15/16 dùng tất cả. Mặc định thực thi tuần tự vì nhiều tasks sửa chung composition/protocol.

## 2. Hợp đồng dùng chung

### 2.1 Metadata

Giữ v1 kind1 LeaderChange,2 TopicCreated legacy,3 ReadBarrier. V2 entry header `version i16, kind u8, controllerEpoch i64`; kind4 FeatureLevel,5 BrokerRegistration,6 BrokerState,7 TopicRecord,8 PartitionRecord. Không tái dùng kind2 cho assignment.

Nested records trong `ClusterRecords` (Task1):

```java
record Endpoint(String host, int port) {}
record Session(int brokerId, UUID storageId, UUID incarnationId, long brokerEpoch) {}
record FeatureLevel(short level) {}
record BrokerRegistration(Session session, Endpoint endpoint, short minVersion, short maxVersion) {}
record BrokerState(int brokerId, long brokerEpoch, boolean fenced) {}
record TopicRecord(UUID topicId, String name, int partitions) {}
record PartitionRecord(UUID topicId, int partitionId, List<Integer> replicas,
                       int leaderId, long leaderEpoch, long partitionEpoch) {}
```

Mỗi payload wrap bởi QuorumEntry nested record cùng tên thêm `long epoch()`. Image lưu `BrokerRegistrationView(registration,fenced,stateOffset)`, stateOffset là exclusive record end của thay đổi lifecycle. brokerEpoch dùng exclusive offset của registration entry được gán tại drain append, không dùng controller epoch. Reservation chưa là identity đã commit.

Payload theo field order trên: IDs/count/port i32; epochs i64; UUID16 bytes; string i32 bytes + strict UTF8; bool u8=0/1; version i16; array count i32 rồi elements. Feature chỉ hỗ trợ level2. MetadataImage thêm metadataVersion, sorted broker/partition maps, giữ appliedOffset()/topics() cho callers lịch sử. Factory `MetadataImage.empty(short version)`; constructor `(long,List<TopicCreated>)` chỉ version1 fixture/reader.

Image v2: magic i32 `0x4d494d32`, schema i16=2, metadataVersion i16, appliedOffset i64, brokers count rồi registration/fenced/revision, topics count rồi descriptors, partitions count rồi records. Sort brokerId, topic UUID unsigned halves, partitionId. Snapshot outer version2 chọn image decoder; không đoán version từ bytes offset cũ. V2 parser từ chối missing partitions/replica references. Snapshot v1 giữ independent vectors.

### 2.2 Control wire v2

Giữ IDs101–109 và CRC v1. Frame v2 thêm senderRole u8 sau senderId: ADMIN=0,VOTER=1,BROKER=2. VOTER bắt buộc voterHash; broker/admin dùng zero hash và không được gọi voter ops. Tất cả phải đúng clusterId. Role/ID pin theo connection. Recompute min envelope theo layout, không dùng69 cho v2.

| ID | Request sau envelope | Success sau ReplyMeta |
|---|---|---|
|110 RegisterBroker|brokerId,storageId,incarnationId,expectedBrokerEpoch,endpoint,min/max version,timeoutMs|Session,registrationOffset,requiredMetadataOffset|
|111 BrokerHeartbeat|Session,sequence i64,appliedOffset i64,recoveryId UUID,recoveryComplete bool,timeoutMs|sessionStatus u8,brokerEpoch i64,stateOffset i64,requiredMetadataOffset i64,recoveryId UUID|
|112 ObserverFetch|Session,knownControllerEpoch i64,nextOffset i64,lastPrefixEpoch i64,maxBytes/maxWaitMs i32|commitOffset i64,DATA=0 hoặc SNAPSHOT=2,batches hoặc SnapshotId|
|113 ObserverSnapshot|Session,knownControllerEpoch i64,SnapshotId,position i64,maxBytes i32|SnapshotId,position/totalLength i64,chunk blob,chunk CRC32C|

DescribeQuorum106 v2 bổ sung canonical voter list/hash để broker pin membership trước tải snapshot; không thay field order106v1. ReadMetadata108 v2 trả full image linearizable để lấy expectedBrokerEpoch; ReadLocalMetadata109 v2 trả local image có label. Không dùng108 mỗi vòng fetch. CreateTopic107 v2 thêm RF i16=1; success UUID + committedOffset. Discovery dùng leader hints nhưng hints không là authority.

Nonzero ReplyMeta.error kết thúc body như v1. Heartbeat session refusal cần offset nên top-level NONE, status ACTIVE=0,FENCED=1,STALE_SESSION=2,REGISTRATION_REQUIRED=3. Status lấy committed image, không pending state. NOT_LEADER14 hiện có tương ứng NOT_CONTROLLER trong spec.

Giữ errors0–19. QuorumError mới: INCOMPATIBLE_METADATA_VERSION20, BROKER_ID_IN_USE21, STORAGE_ID_MISMATCH22, STALE_BROKER_EPOCH23, NO_ELIGIBLE_BROKER24. Code23 dành registration CAS; heartbeat sai phiên dùng structured status. Data ErrorCode thêm cùng nghĩa/số16 CLUSTER_MISMATCH,23 STALE_BROKER_EPOCH,24 NO_ELIGIBLE_BROKER,25 NOT_PARTITION_LEADER,26 FENCED_BROKER,27 STALE_PARTITION_EPOCH. Không tái sử dụng số14–19 cho nghĩa khác.

### 2.3 Data wire v2

Giữ envelope operation/version/requestId, ops1–4 và wire batch v1. Mở rộng Protocol sealed permits cho nested DTOs ClusterProtocol; reuse RequestFrame/ResponseFrame/transport. Production dispatcher chỉ nhận v2, v1 trả UNSUPPORTED_VERSION; v1 vectors giữ nguyên.

```java
// Task13: nested ClusterProtocol records
record Route(Protocol.TopicPartition partition, int brokerId, long brokerEpoch, long leaderEpoch) {}
record CreateTopic(UUID clusterId, String name, int partitions, short replicationFactor,
                   int timeoutMs) implements Protocol.Request {}
record Metadata(UUID clusterId, List<String> names) implements Protocol.Request {}
record ProduceEntry(Route route, Protocol.Batch batch) {}
record Produce(UUID clusterId, Protocol.AckMode ack, int timeoutMs,
               List<ProduceEntry> entries) implements Protocol.Request {}
record FetchEntry(Route route, long offset, int maxBytes) {}
record Fetch(UUID clusterId, int maxBytes, int minBytes, int maxWaitMs,
             List<FetchEntry> entries) implements Protocol.Request {}
```

Bootstrap Metadata cho phép UUID zero trước pin; các request khác yêu cầu clusterId. Reply bắt đầu Protocol.Error. MetadataReply fields: Error,clusterId,appliedOffset,brokers array(id,endpoint,brokerEpoch,fenced),topics array(name,UUID,partitions(id,error,replicas,leaderId,leaderEpoch,partitionEpoch)). Java nested DTOs `BrokerInfo`, `TopicInfo`, `PartitionInfo` phản ánh đúng fields trên. CreateTopicReply: Error,UUID,commitOffset. ProduceResult: partition,error,outcome u8 SUCCESS=0/REJECTED=1/UNKNOWN=2,firstOffset,nextOffset. ProduceReply: Error,List<ProduceResult>. FetchResult: partition,error,start,LEO,HW i64,List<Protocol.FetchBatch>; FetchReply: Error,List<FetchResult>.

Outcome.REJECTED chứng minh trước append. Timeout/storage error sau possible append là UNKNOWN; invalid combination/error enum trên reply thành UNKNOWN nếu client đã gửi Produce. Request/response strings, lists và UUID dùng encoding §2.1. Error offsets=-1, SUCCESS chỉ đi cùng NONE.

Oversized first batch exception là budget toàn Fetch operation, không mỗi broker. Trong v2 Fetch thêm trailing `boolean allowOversizedFirstBatch` sau entries; convenience constructor sáu fields trên mặc định false, ClusterClient chủ động cấp true cho đúng một subrequest. Broker v2 chỉ dùng exception khi flag=true. Global client scheduler cấp exception trước cho một entry xác định; các broker khác chạy sau nếu chưa tiêu hết budget. Không thêm field vào v1.

### 2.4 Broker persistence

Root: `broker-identity.bin`, `.broker.lock`, `observer/`, `partition-inventory.journal`, `partitions/<topicUUID>/<partitionId>/`. Format chỉ root mới/rỗng. Identity magic0x42494432,version i16=2,length i32,clusterUUID,brokerId i32,storageUUID,CRC32C. Force file và directory trước báo success.

Inventory frame: magic0x50494e32,version i16=2,length i32,sequence i64,type u8 INIT=0/INTENT=1/COMPLETE=2,payload,CRC32C. INIT payload storageUUID; INTENT/COMPLETE payload topicUUID+partitionId i32. Force INTENT trước tạo log, force log/directory rồi COMPLETE trước ready. Missing inventory của root đã format là fatal. Cap64MiB/frame64KiB, exhaustion fail explicit.

Observer journal dùng StateJournal framing, không HARD_STATE: GENERATION/COMMIT/SNAPSHOT_SET/PREFIX_INTENT/PREFIX_DONE. Format tạo empty generation/log tại0. Receive: append/flush → checkpoint verified commit → apply → publish image. Restart chỉ replay tới checkpoint. Snapshot: download/checksum → force file/directory → drain old I/O → create/force empty log tạiS → force GENERATION → publish image. Không giữ suffix cũ chưa chứng minh khớp; giữ hai snapshots, pin files theo Phase3. Journal chỉ publish files đã durable.

## Task 1: Metadata records, limits và encoding v2

**Files:** Create `M/controller/metadata/ClusterRecords.java`, `M/controller/metadata/MetadataLimits.java`; modify `M/controller/log/QuorumEntry.java`, `M/controller/log/QuorumEntryCodec.java`; create `T/controller/log/ClusterEntryCodecTest.java`, `R/controller/protocol-v2-entry-vectors.txt`, `docs/controller-storage-v2.md`.

**Interfaces:** Records §2.1; `MetadataLimits(int maxBrokers,int maxTopics,int maxPartitions,int maxImageBytes)`, `static defaults()`, `long maxEncodedImageBytes()`. Overload `QuorumEntryCodec.encode(QuorumEntry,short)`/`decode(byte[],MetadataLimits)`; preserve v1 methods.

- [ ] Viết independent vector test và separate tests cho incarnation/storageID/partition epochs.

```java
@Test void featureVectorHasStableBytes() throws Exception {
    var entry = new QuorumEntry.FeatureLevel(0, new ClusterRecords.FeatureLevel((short) 2));
    assertArrayEquals(HexFormat.of().parseHex("00020400000000000000000002"),
        QuorumEntryCodec.encode(entry, (short) 2));
}
```

- [ ] Run `mvn -Dtest=ClusterEntryCodecTest test`; RED missing v2 API.
- [ ] Implement validations UUID nonzero, IDs>=0, epochs>=0, RF1/leader membership, version min<=max. Reject unknown kind, trailing data, overflow/count before allocate. Defensive copies. Conservative image size bound:

```java
long bytes = Math.addExact(64L,
    Math.addExact(Math.multiplyExact(maxBrokers, 512L),
    Math.addExact(Math.multiplyExact(maxTopics, 320L), Math.multiplyExact(maxPartitions, 128L))));
if (bytes > maxImageBytes) throw new IllegalArgumentException("Image budget");
```

- [ ] Add exact size preflight vs encoding, UTF8, truncated/oversized/unknown-version tests. Run `mvn -Dtest=ClusterEntryCodecTest,QuorumEntryCodecTest,QuorumCodecTest test`; GREEN and v1 vectors unchanged.
- [ ] Stage task paths, check diff; commit `feat(controller): add versioned cluster metadata records`.

## Task 2: Atomic image apply và snapshot schema v2

**Files:** Modify `M/controller/metadata/MetadataImage.java`, `M/controller/metadata/MetadataStateMachine.java`, `M/controller/metadata/MetadataImageCodec.java`, `M/controller/snapshot/SnapshotStore.java`, `M/controller/snapshot/SnapshotCoordinator.java`, `M/controller/ControllerConfig.java`; create `T/controller/metadata/ClusterMetadataApplyTest.java`, `T/controller/metadata/ClusterImageCodecTest.java`; extend `T/controller/snapshot/SnapshotStoreTest.java`, `T/controller/snapshot/SnapshotInstallCrashTest.java`, `T/controller/ControllerConfigTest.java`.

**Interfaces:** `MetadataStateMachine(MetadataLimits,short version)`, no-arg legacy fixture. Image maps/registration view §2.1. `MetadataImageCodec.encodeV2(MetadataImage,MetadataLimits): byte[]`, `decodeV2(byte[],MetadataLimits): MetadataImage`. Nested test helper `featureBatch()` creates base0 epoch0 one FeatureLevel QuorumBatch.

- [ ] Viết RED atomic completeness test:

```java
@Test void incompleteTopicBatchPublishesNothing() throws Exception {
    var state = new MetadataStateMachine(MetadataLimits.defaults(), (short) 2);
    state.apply(featureBatch());
    var before = state.image();
    var topic = new QuorumEntry.TopicRecord(0,
        new ClusterRecords.TopicRecord(new UUID(0, 7), "orders", 2));
    assertThrows(IOException.class, () -> state.apply(
        new QuorumBatch(before.appliedOffset(), List.of(topic))));
    assertSame(before, state.image());
}
```

- [ ] Run `mvn -Dtest=ClusterMetadataApplyTest test`; RED.
- [ ] Implement private copy then validate complete batch/cross-references, publish once. Private methods in MetadataStateMachine:

```java
MetadataImage next = applyToPrivateCopy(image, batch);
validateCompleteImage(next);
image = next;
```

Topic duplicate/conflict semantics, wrong session and decreasing epochs checked before publication. Feature duplicate same level idempotent, change level forbidden. Batch invalid committed history fails node, not skip entry.
- [ ] Implement versioned snapshot envelope and image codec; replace hard-coded128/256KiB slice assumptions with limits and bounded streaming/chunks. Test maximum image, image>256KiB under valid nondefault limits, broker lifecycle offsets/features preserved, v1 decode still works.
- [ ] Run `mvn -Dtest=ClusterMetadataApplyTest,ClusterImageCodecTest,MetadataStateMachineTest,SnapshotStoreTest,SnapshotInstallCrashTest,ControllerConfigTest test`; GREEN. Commit `feat(controller): apply cluster metadata atomically`.

## Task 3: Broker control RPC và role isolation

**Files:** Create `M/controller/protocol/BrokerControlProtocol.java`; modify `M/controller/protocol/QuorumProtocol.java`, `M/controller/protocol/QuorumCodec.java`, `M/controller/protocol/QuorumError.java`, `M/controller/transport/QuorumFrameDecoder.java`, `M/controller/transport/NettyQuorumTransport.java`, `M/controller/ControllerNode.java`, `M/controller/ControllerService.java`; create `T/controller/protocol/BrokerControlCodecTest.java`, `T/controller/transport/BrokerRoleIsolationTest.java`, `R/controller/protocol-v2-vectors.txt`, `docs/controller-protocol-v2.md`.

**Interfaces:** BrokerControlProtocol nested Register/RegisterReply/Heartbeat/HeartbeatReply/ObserverFetch/ObserverFetchReply/ObserverSnapshot/ObserverSnapshotReply implement QuorumProtocol.Request/Reply using §2.2 fields. SenderRole enum ADMIN/VOTER/BROKER. Frame canonical constructor `(short version,SenderRole,short operation,boolean response,UUID clusterId,int senderId,long requestId,byte[] voterHash,Message)`; old constructor delegates version1. `static allowed(Frame): boolean`. Expose bounded `ControllerService.request(Request,long): CompletableFuture<Reply>` for new commands.

- [ ] Write independent golden vectors and role test:

```java
@Test void brokerCannotVote() {
    var frame = new QuorumProtocol.Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER,
        (short) 101, false, new UUID(0, 1), 1, 7, new byte[32],
        new QuorumProtocol.Vote(1, 0, 0));
    assertFalse(BrokerControlProtocol.allowed(frame));
}
```

- [ ] Run `mvn -Dtest=BrokerControlCodecTest,BrokerRoleIsolationTest test`; RED.
- [ ] Implement preflight and role whitelist: ADMIN106–109, VOTER101–109, BROKER106–113. No broker path into PeerRequest vote/fetch104. Bind role/ID/socket incarnation; wrong cluster rejected before consensus mutation. Structured session errors retain offsets, no exception swallowing body.
- [ ] Test wrong hash/role/cluster, response arriving old connection, array preflight, encodedSize, release-on-disconnect. Run `mvn -Dtest=BrokerControlCodecTest,BrokerRoleIsolationTest,QuorumCodecTest,QuorumCodecLimitsTest,QuorumTransportTest,QuorumTransportBudgetTest,QuorumOutboundOwnershipTest test`; GREEN.
- [ ] Commit `feat(protocol): add broker control RPCs and roles`.

## Task 4: Feature bootstrap, registration và atomic assignment

**Files:** Create `M/controller/metadata/ClusterControlManager.java`; modify `M/controller/consensus/QuorumStateMachine.java`, `M/controller/consensus/QuorumEvent.java`, `M/controller/consensus/QuorumEffect.java`, `M/controller/ControllerService.java`, `M/controller/ControllerNode.java`, `M/controller/ControllerOptions.java`, `M/controller/ClusterIdentity.java`, `M/controller/persistence/QuorumStateStore.java`; create `T/controller/metadata/ClusterControlManagerTest.java`, `T/controller/consensus/ClusterAdmissionTest.java`; extend `T/controller/support/QuorumHarness.java`, `T/controller/support/FakeDisk.java`, `docs/controller-storage-v2.md`.

**Interfaces:** `ClusterControlManager(MetadataLimits,IntPredicate eligible)`, `onImage(MetadataImage)`, `drainNext(long controllerEpoch,long baseOffset): Optional<QuorumBatch>`, `onLeadershipLost()`. Runs on consensus loop. Manager holds reservations/invocation→batchEnd; QuorumStateMachine still owns append/flush/commit. drainNext lấy một lệnh nguyên tử, cấp exact baseOffset khi chuẩn bị Append effect; xử lý tiếp sau append completion. Không flatten nhiều atomic commands rồi chia lại theo byte budget; read barrier được append sau các lệnh mà nó cần bao phủ. `static assign(List<Integer>,Map<Integer,Integer>,int): List<Integer>`. Extend harness `(long seed,short metadataVersion)`, default legacy fixture.

- [ ] Write RED tests pending loads and registration idempotence:

```java
@Test void allocationAccountsForPendingAndTieBreaksById() {
    assertEquals(List.of(2, 3, 2, 3, 1), ClusterControlManager.assign(
        List.of(1,2,3), Map.of(1,2,2,0,3,0), 5));
}
```

- [ ] Run `mvn -Dtest=ClusterControlManagerTest,ClusterAdmissionTest test`; RED.
- [ ] Format v2 stores desired metadata.version2 in identity; extend ClusterIdentity and its identity encoder/decoder with explicit format-version dispatch, preserving v1 fixture constructors. First leader appends LeaderChange + FeatureLevel if absent; admin-ready only after feature commit/apply. Failover replay prevents duplicate bootstrap. Existing v1 fixture mode stays only for history regressions.
- [ ] Implement CAS expectedBrokerEpoch, idempotent current incarnation, storageId mismatch/version rejection, pending reservations. Allocate brokerEpoch at exact append offset; complete registration only after Applied covers it.
- [ ] Implement assignment with min(committed+pending load), id tie-break; reserve whole topic and append atomic batch. Do not let existing DrainProposals split it. Eligibility predicate supplied by Task5; unit tests use explicit predicate. Complete CreateTopic after apply, not provisioning.

```java
int selected = eligible.stream().min(Comparator
    .comparingInt((Integer id) -> loads.getOrDefault(id, 0))
    .thenComparingInt(Integer::intValue)).orElseThrow();
loads.merge(selected, 1, Math::addExact);
```

- [ ] Cases: empty eligible set, duplicate/conflict/pending capacity, leader change with inherited proposal, timeout then commit, maximum partition count, session unfence plus partition epoch changes fit one batch. Reject config too small for required lifecycle batch at startup. Run `mvn -Dtest=ClusterControlManagerTest,ClusterAdmissionTest,QuorumAdmissionTest,PendingTopicLeadershipTest,CommitRuleTest,LinearizableReadTest test`; GREEN.
- [ ] Commit `feat(controller): commit broker sessions and assignments`.

## Task 5: Heartbeat liveness, committed fencing và catch-up target

**Files:** Create `M/controller/metadata/HeartbeatTracker.java`; modify `M/controller/metadata/ClusterControlManager.java`, `M/controller/consensus/QuorumStateMachine.java`, `M/controller/ControllerConfig.java`, `M/controller/ControllerOptions.java`; create `T/controller/metadata/HeartbeatTrackerTest.java`, `T/controller/consensus/BrokerFencingTest.java`.

**Interfaces:** `HeartbeatTracker(long sessionTimeoutNanos)`, `leaderStarted(long epoch,long now)`, `record(Session,long sequence,long now): boolean`, `eligible(int brokerId,long now): boolean`, `expired(long now): Set<Integer>`. Manager replies with Task3 HeartbeatReply. RecoveryId identifies one bounded session handshake; same ID retry shares pending unfence.

- [ ] Write duplicate-sequence RED test:

```java
@Test void duplicateCannotExtendSession() {
    var t = new HeartbeatTracker(10);
    var s = new ClusterRecords.Session(1, new UUID(0,1), new UUID(0,2), 4);
    t.leaderStarted(3, 0);
    assertTrue(t.record(s, 1, 0));
    assertFalse(t.record(s, 1, 9));
    assertTrue(t.expired(10).contains(1));
}
```

- [ ] Run `mvn -Dtest=HeartbeatTrackerTest,BrokerFencingTest test`; RED.
- [ ] Implement tracker based on fresh accepted sequence/current session; no log writes for normal heartbeat. Leader startup observation window10s, no inherited monotonic timestamps; expiration only proposes durable fence. Broker messages never update existing quorum voter contact/challenge tracker.
- [ ] Implement new recoveryId capturing fixed requiredMetadataOffset, wait applied>=target and recoveryComplete, append unfence newer than blocked offset. A same-session recovery confirmation creates fresh BrokerState even already unfenced; deduplicate by recoveryId pending plus last-completed result in bounded session memory. After leader change fresh ID required, no reuse old volatile target.

```java
// Handler on consensus loop; image is committed, pending state is separate.
if (heartbeat.appliedOffset() >= target && heartbeat.recoveryComplete()) {
    enqueueUnfenceForCurrentRecovery(heartbeat); // private ClusterControlManager method
} else {
    replyWithCapturedTarget(heartbeat);         // private, no append/barrier
}
```

- [ ] Tests idle loop leaves logEnd unchanged; stale heartbeat cannot cancel pending fence; minority cannot commit unfence; lost response/retry; new leader excludes broker until fresh heartbeat; recovery target doesn't chase unrelated appends. Run `mvn -Dtest=HeartbeatTrackerTest,BrokerFencingTest,ElectionTest,ReplicationValidationTest,LinearizableReadTest test`; GREEN.
- [ ] Commit `feat(controller): track broker liveness without heartbeat log writes`.

## Task 6: Observer endpoint with committed-only reads

**Files:** Create `M/controller/metadata/ObserverReadService.java`; modify `M/controller/ControllerNode.java`, `M/controller/consensus/QuorumStateMachine.java`, `M/controller/consensus/QuorumEffect.java`, `M/controller/runtime/EffectRunner.java`, `M/controller/snapshot/SnapshotStore.java`; create `T/controller/metadata/ObserverReadServiceTest.java`, `T/controller/transport/ObserverBudgetTest.java`.

**Interfaces:** `static committedPrefix(List<QuorumBatch>,long commitOffset): List<QuorumBatch>`; route Task3 DTO via same bounded service/ReplyRoute. New ReadObserver effect carries frozen upperBound, session, request generation. Completion rechecks leader/session/generation; reply body never includes uncommitted tail.

- [ ] Write RED boundary test:

```java
@Test void observerCannotSeeUncommittedTail() {
    var a = new QuorumBatch(0, List.of(new QuorumEntry.ReadBarrier(1)));
    var b = new QuorumBatch(1, List.of(new QuorumEntry.ReadBarrier(1)));
    assertEquals(List.of(a), ObserverReadService.committedPrefix(List.of(a,b), 1));
}
```

- [ ] Run `mvn -Dtest=ObserverReadServiceTest,ObserverBudgetTest test`; RED.
- [ ] Filter entire contiguous batches ending<=frozen committed boundary; never trim record count within batch. Offset before retained prefix returns snapshot. Committed prefix epoch mismatch returns explicit consistency failure, never asks observer to truncate applied history. Fenced current session may fetch to catch up.

```java
return batches.stream().takeWhile(b -> b.nextOffset() <= commitOffset).toList();
```

- [ ] Add bounded100ms long-poll, one outstanding/session, capacity maxBrokers. Charge raw/decoded/read/outbound bytes, observer only shared pool; reuse max2 snapshot upload pins/controller and30s expiration. Close/epoch change releases references once. No voter-reserved capacity consumed by observer bulk bytes.
- [ ] Tests stale disk completion, expired snapshot IDs, wrong prefix, legal max batch, 32 observers saturated while voter majority still progresses. Run `mvn -Dtest=ObserverReadServiceTest,ObserverBudgetTest,QuorumTransportBudgetTest,SnapshotStoreTest,ReplicationTest test`; GREEN.
- [ ] Commit `feat(controller): serve bounded committed metadata observers`. Checkpoint A: `mvn test`, preserve v1 codec/core suites.

## Task 7: Broker identity, explicit format và cluster config

**Files:** Create `M/broker/cluster/BrokerIdentityStore.java`, `M/broker/cluster/BrokerClusterConfig.java`; extend `M/broker/BrokerMain.java`, `M/broker/BrokerConfig.java`; create `T/broker/cluster/BrokerIdentityStoreTest.java`, `T/broker/cluster/BrokerClusterConfigTest.java`; modify `T/broker/BrokerMainTest.java`.

**Interfaces:** nested `BrokerIdentityStore.Identity(UUID clusterId,int brokerId,UUID storageId)`; `format(Path,UUID,int,DurableFiles): Identity`, `open(Path,UUID,int,DurableFiles): BrokerIdentityStore`, `identity()`, `close()`. Own root lock until all broker workers drained. BrokerClusterConfig record `(UUID clusterId,int brokerId,Endpoint advertised,List<Endpoint> controllers,Duration heartbeatInterval,Duration heartbeatRpcTimeout,Duration sessionTimeout,MetadataLimits limits)`; storage/data-worker config stays BrokerConfig.

- [ ] RED test startup never silently creates identity:

```java
@Test void openingUnformattedRootFails(@TempDir Path root) {
    assertThrows(IOException.class, () -> BrokerIdentityStore.open(
        root, new UUID(0,1), 7, new DurableFiles()));
}
```

DurableFiles là concrete class với public no-arg constructor; tests cần successful directory forces trên Windows dùng FaultFiles, không skip logic tests. QuorumBatch constructor hiện có là (long baseOffset,List<QuorumEntry> entries), epoch lấy từ entries.
- [ ] Run `mvn -Dtest=BrokerIdentityStoreTest,BrokerClusterConfigTest test`; RED.
- [ ] Implement layout §2.4, forced identity and root entries, create initialized observer/inventory durable roots as part format (initialization methods Task8/11 wired there before production switch). Until Task8/11 land, test identity format via dedicated fixture root; never expose partially formatted root as runnable.

```java
if (Files.exists(root) && Files.list(root).findAny().isPresent()) {
    throw new IOException("Format requires an empty data root");
}
```

Use try-with-resources for Files.list in source. Startup missing components, v1 root, wrong ID/cluster/version fail; data directory lock held through failure cleanup.
- [ ] Add CLI `BrokerMain format --config <path> --data <root>` and manifest-only format tests; parse controller bootstrap/advertised endpoint, timeout ordering and limits. Config default names: cluster.id, broker.id, advertised.host, advertised.port, controller.bootstrap.servers, broker.heartbeat.interval.ms, broker.heartbeat.rpc.timeout.ms, broker.session.timeout.ms.
- [ ] Tests nonempty root unchanged, two opens fail lock, force failure never ready, invalid UTF8 host/ports/timeouts. Run `mvn -Dtest=BrokerIdentityStoreTest,BrokerClusterConfigTest,BrokerMainTest,BrokerConfigTest test`; GREEN. Commit `feat(broker): format cluster identity explicitly`.

## Task 8: Durable observer generation và reusable snapshot journal

**Files:** Create `M/controller/snapshot/SnapshotJournal.java`, `M/broker/cluster/ObserverStore.java`; modify `M/controller/snapshot/SnapshotStore.java`, `M/controller/persistence/QuorumStateStore.java`, `M/broker/cluster/BrokerIdentityStore.java`; create `T/broker/cluster/ObserverStoreTest.java`, `T/broker/cluster/ObserverInstallCrashTest.java`; extend `T/controller/snapshot/SnapshotPublicationTest.java`.

**Interfaces:** `SnapshotJournal.frames(): List<StateJournal.Frame>`, `append(short,byte[]): long`; adapters over QuorumStateStore.journal and ObserverStore journal. SnapshotStore overload accepts SnapshotJournal; SnapshotJournal adapter giữ existing type6 SNAPSHOT_SET bytes; preserve old constructor delegation. Shared snapshot validation dùng overload nhận UUID clusterId và byte[] voterHash thay cho full ClusterIdentity; controller constructor delegate hai fields này. Broker lấy expected identity sau metadata discovery, không masquerade voter. Persist verified fixed membership in observer manifest before accepting controller snapshot; initial handshake DescribeQuorum v2 includes voter list/hash for validation.

ObserverStore APIs: `static format(Path,UUID,DurableFiles,LogConfig)`, `static open(Path,UUID,DurableFiles,LogConfig,MetadataLimits)`, `appendCommitted(List<QuorumBatch>,long verifiedCommit)`, `pinMembership(byte[] voterHash)`, `install(SnapshotId,MetadataImage,BooleanSupplier installAllowed)`, `image()`, `generation(): UUID`, `durableEnd(): long`, `close()`. Single ordered worker owns all mutations. No vote/epoch hard state writes; controller epoch tracking for response correlation stays separate.

- [ ] RED test rejects a batch beyond verified committed coverage before any append:

```java
@Test void cannotPersistUncommittedBatch(@TempDir Path root) throws Exception {
    var files = new FaultFiles();
    ObserverStore.format(root, new UUID(0,1), files, LogConfig.defaults());
    try (var store = ObserverStore.open(root,new UUID(0,1),files,
            LogConfig.defaults(),MetadataLimits.defaults())) {
        var batch = new QuorumBatch(0,List.of(new QuorumEntry.ReadBarrier(1)));
        assertThrows(IOException.class, () -> store.appendCommitted(List.of(batch), 0));
        assertEquals(0, store.durableEnd());
    }
}
```

- [ ] Run `mvn -Dtest=ObserverStoreTest,ObserverInstallCrashTest test`; RED.
- [ ] Implement framing/publication ordering §2.4; reuse QuorumLog primitives and strict recovery, not controller election state. MetadataStore checkpoint end is downloaded contiguous batch end, never leaderCommit beyond received coverage; empty reply may update verified remote commit separately without persistence claim.

```java
if (batch.nextOffset() > verifiedCommit) throw new IOException("Uncommitted observer batch");
log.appendReplica(batch);
log.flush();
journal.append(StateJournal.COMMIT, ByteBuffer.allocate(8).putLong(batch.nextOffset()).array());
metadata.apply(batch);
```

Implement private fields log/journal/metadata in ObserverStore. Journal forces in append. Restart replay only checkpoint; a durable received tail beyond checkpoint may be validated and fetched again, never published before commit coverage validation.
- [ ] Extract SnapshotJournal only, preserve controller semantics. Install/GC tests crash at each force/rename/manifest/delete, select old/new generation, reject published corruption, no local votes file, retained two snapshots and pins. With disk pending shutdown cannot release root lock.
- [ ] Run `mvn -Dtest=ObserverStoreTest,ObserverInstallCrashTest,SnapshotPublicationTest,SnapshotInstallCrashTest,GenerationRecoveryTest,PartitionNonzeroBaseTest test`; GREEN. Commit `feat(broker): persist observer metadata generations`.

## Task 9: Controller channel và metadata pull loop

**Files:** Create `M/broker/cluster/BrokerControlClient.java`, `M/broker/cluster/MetadataObserver.java`; extend `M/controller/client/ControllerClientTransport.java`, `M/controller/client/NettyControllerClientTransport.java` for role/version, preserve admin role defaults; create `T/broker/cluster/BrokerControlClientTest.java`, `T/broker/cluster/MetadataObserverTest.java`.

**Interfaces:** `BrokerControlClient(BrokerClusterConfig,ControllerClientTransport.Factory,DeadlineScheduler)`, `request(QuorumProtocol.Request,long deadlineNanos): CompletableFuture<QuorumProtocol.Reply>`, `close()`. `MetadataObserver(BrokerControlClient,ObserverStore,DeadlineScheduler,Executor disk,Consumer<MetadataImage> published,Consumer<Throwable> fatal)`, `start(Session)`, `stop(): CompletableFuture<Void>`, `image()`. Observer rejects received>end coverage and correlates controller epoch/session/generation/requestId; one fetch/download.

- [ ] RED test uses `T/broker/cluster/ScriptedControlTransport.java` new fixture implementing existing ControllerClientTransport. Methods `replyNext(Reply)`, `disconnect()`, `sent(): List<Request>`; retains real correlation/encoding path, manually advances ManualScheduler.

```java
@Test void observerDoesNotFetchAheadOfDurableCompletion() throws Exception {
    try (var h = new ObserverHarness()) { // fixture defined in this task
        h.start();
        h.completeFetchWithOneCommittedBatch();
        assertEquals(1, h.transport().sent().size());
        h.runOneDiskTask();
        h.runScheduled();
        assertEquals(2, h.transport().sent().size());
    }
}
```

`T/broker/cluster/ObserverHarness.java` owns formatted temp ObserverStore, ManualScheduler, ScriptedControlTransport, queued Executor and fixed Session; APIs in snippet execute actual observer/store. Its one batch is FeatureLevel at0 with commit1; helpers do not fabricate publication.
- [ ] Run `mvn -Dtest=BrokerControlClientTest,MetadataObserverTest test`; RED.
- [ ] Implement discovery/bootstrap and broker role, same deadline across hint redirects, backoff100ms→1s capped with injected jitter source. Use 106/108 for first membership/session setup, thereafter112/113. Factory creates a transport per connection attempt; pool one live broker-role connection per selected controller with a cap3, discard/close abandoned attempt and fence callbacks by generation. Transport methods accept role Frame, no API to weaken voter validation. Reject foreign cluster/higher unknown metadata.version before disk/apply.

```java
long requested = store.durableEnd();
// request nextOffset=requested; apply next request only after ordered append/checkpoint/apply completion.
control.request(fetchRequest(requested), deadline).whenComplete(this::onFetchCompletion);
```

Private fetchRequest/onFetchCompletion marshal to observer loop, never write disk in callback. Snapshot chunk repeats must match bytes/position; leader/session changes cancel old transfer and close refs.
- [ ] Test delayed old replies, timeout/disconnect, snapshot catch-up, chunk corruption/retry, fatal local durability error, one in-flight, bounded backlog, stop drains disk. Run `mvn -Dtest=BrokerControlClientTest,MetadataObserverTest,ObserverStoreTest,ControllerClientRetryTest test`; GREEN.
- [ ] Commit `feat(broker): follow committed metadata from controllers`.

## Task 10: Broker lifecycle và serving gate không có lease

**Files:** Create `M/broker/cluster/BrokerLifecycle.java`, `M/broker/cluster/ServingGate.java`; create `T/broker/cluster/BrokerLifecycleStateTest.java`, `T/broker/cluster/ServingGateTest.java`; modify `M/broker/cluster/BrokerControlClient.java`, `M/broker/cluster/MetadataObserver.java`.

**Interfaces:** `BrokerLifecycle.State` STARTING/RECOVERING/FENCED/RUNNING/FAILED/STOPPING. `BrokerLifecycle(BrokerIdentityStore.Identity,BrokerControlClient,MetadataObserver,DeadlineScheduler,ServingGate)`, `start(): CompletableFuture<Void>`, `stop(): CompletableFuture<Void>`, `state()`. `ServingGate(Session)`; `apply(long imageOffset,long lifecycleRevision,boolean fenced)`, `reject(long stateOffset)`, `canServe(): boolean`, `close()`. Gate tracks latest applied lifecycle revision and blocked offset; atomically readable by data workers. No heartbeat timestamp in gate.

- [ ] RED stale-response test:

```java
@Test void oldFenceCannotUndoNewerUnfence() {
    var s = new ClusterRecords.Session(1,new UUID(0,1),new UUID(0,2),4);
    var gate = new ServingGate(s);
    gate.apply(10, 10, false);
    gate.reject(8);
    assertTrue(gate.canServe());
    gate.reject(12);
    assertFalse(gate.canServe());
    gate.apply(12, 10, false);
    assertFalse(gate.canServe());
    gate.apply(14, 14, false);
    assertTrue(gate.canServe());
}
```

- [ ] Run `mvn -Dtest=BrokerLifecycleStateTest,ServingGateTest test`; RED.
- [ ] Implement lock/recovery → register incarnation once → observer catch-up → recoveryComplete heartbeat → committed unfence apply → RUNNING. Session replacement invalidates old gate; never reset brokerEpoch from stale response. New recoveryId for real recovery/leader switch, not every heartbeat.

```java
// ServingGate.apply; caller first validates session and monotonic revision.
if (lifecycleRevision > blockedOffset && imageOffset >= lifecycleRevision) {
    allowed = !fenced;
}
// Heartbeat timeout changes diagnostics/retry only, never allowed/blockedOffset.
```

blockedOffset starts -1; accepted rejection increases it. Cache unfenced for previous incarnation cannot grant new process permission. Rejection older than applied lifecycle revision ignored. Permanent close cannot be reversed by apply.
- [ ] Tests heartbeat absent60s remains RUNNING; startup no quorum stays FENCED; known fence/rejection stops; NOT_LEADER/OVERLOADED does not; fixed target; heartbeat success cannot reopen without new decision; shutdown gate close. Use ManualScheduler, no Thread.sleep.
- [ ] Run `mvn -Dtest=BrokerLifecycleStateTest,ServingGateTest,MetadataObserverTest test`; GREEN. Commit `feat(broker): gate serving on committed session state`.

## Task 11: Inventory và partition reconciliation

**Files:** Create `M/broker/cluster/PartitionInventory.java`, `M/broker/cluster/ClusterPartitionManager.java`; modify `M/broker/PartitionRegistry.java`, `M/broker/FilePartitionStore.java`, `M/broker/cluster/BrokerIdentityStore.java`; create `T/broker/cluster/PartitionInventoryTest.java`, `T/broker/cluster/PartitionProvisioningCrashTest.java`, `T/broker/cluster/ClusterPartitionManagerTest.java`.

**Interfaces:** `PartitionInventory.format(Path,UUID,DurableFiles)`, `open(Path,UUID,DurableFiles)`, `begin(TopicPartition)`, `complete(TopicPartition)`, `state(TopicPartition): State` NEW/INTENT/COMPLETE, `close()`. Unknown entry only NEW after journal INIT validated. `ClusterPartitionManager.reconcile(MetadataImage,Session): CompletableFuture<Void>`, `runtime(TopicPartition): Optional<PartitionRuntime>`, `closeAsync(): CompletableFuture<Void>`; constructor takes inventory, PartitionStore.Factory, ordered executor, root and gate.

- [ ] RED completion and missing-directory tests:

```java
@Test void completionSurvivesReopen(@TempDir Path root) throws Exception {
    UUID storage = new UUID(0,9);
    var tp = new Protocol.TopicPartition(new UUID(0,7),0);
    var files = new FaultFiles();
    PartitionInventory.format(root,storage,files);
    try (var inv = PartitionInventory.open(root,storage,files)) {
        inv.begin(tp);
        inv.complete(tp);
    }
    try (var inv = PartitionInventory.open(root,storage,files)) {
        assertEquals(PartitionInventory.State.COMPLETE,inv.state(tp));
    }
}
```

- [ ] Run `mvn -Dtest=PartitionInventoryTest,PartitionProvisioningCrashTest,ClusterPartitionManagerTest test`; RED.
- [ ] Implement §2.4 framing, failure poisoning, valid partial-tail recovery only. Format inventory before root identity publication. NEW→INTENT force→create/flush/force directory→COMPLETE force→ready. No appends before COMPLETE.

```java
if (inventory.state(tp) == PartitionInventory.State.COMPLETE && !Files.isDirectory(path)) {
    markUnavailable(tp, "Previously provisioned log missing");
    return;
}
```

Private manager markUnavailable records diagnostic; no failure of unrelated logs. INTENT resumes only before ever ready; existing files undergo strict recovery, never delete to force success.
- [ ] Async reconcile tokens bind session/image assignment/runtime generation. Obsolete open completion closes runtime without publication; metadata loop never blocks on opens. Missing inventory fatal; stray directory retained/unserved; UUID identity never replaced by same topic name.
- [ ] Crash each force/append/open/completion; missing COMPLETE directory/inventory, corruption, one disk failure, blocked shutdown retains lock. Run `mvn -Dtest=PartitionInventoryTest,PartitionProvisioningCrashTest,ClusterPartitionManagerTest,PartitionTruncateFailureTest,PartitionStrictDurabilityTest test`; GREEN.
- [ ] Commit `feat(broker): provision assigned logs with durable inventory`.

## Task 12: Cluster composition và forwarding

**Files:** Create `M/broker/metadata/BrokerMetadata.java`, `M/broker/metadata/ClusterMetadataService.java`; modify `M/broker/Broker.java`, `M/broker/BrokerMain.java`, `M/broker/RequestDispatcher.java`, `M/broker/metadata/MetadataService.java`; migrate `T/broker/BrokerLifecycleTest.java`, `T/broker/BrokerIsolationTest.java`, `T/broker/RequestDispatcherTest.java`, `T/broker/metadata/MetadataServiceTest.java`, `T/broker/metadata/MetadataRecoveryTest.java`; create `T/broker/metadata/ClusterMetadataServiceTest.java`, `T/broker/cluster/ClusterBrokerLifecycleTest.java`.

**Interfaces:** `BrokerMetadata.image(): MetadataImage`, `create(String,int,long deadlineNanos): CompletableFuture<BrokerControlProtocol.CreateResult>`; nested CreateResult(UUID topicId,long committedOffset) represents107v2 result. `ClusterMetadataService(BrokerControlClient,Supplier<MetadataImage>,DeadlineScheduler)`. `Broker.start(BrokerConfig,BrokerClusterConfig)` replaces production standalone. Dispatcher uses BrokerMetadata. Actual v2 branches Task13; this task tests service/composition directly.

- [ ] RED test via new `T/broker/metadata/ForwardingHarness.java`, owns real service/control client + Task9 scripted transport and ManualScheduler. `service()`, `replyCommitted(UUID,long)` and `localMetadataAppends()`; last counter records attempted use of forbidden local authority, not synthetic response.

```java
@Test void topicIdComesFromController() throws Exception {
    try (var h = new ForwardingHarness()) {
        var pending = h.service().create("orders",3,30_000_000_000L);
        UUID id = new UUID(0,17);
        h.replyCommitted(id,20);
        assertEquals(id,pending.join().topicId());
        assertEquals(0,h.localMetadataAppends());
    }
}
```

- [ ] Run `mvn -Dtest=ClusterMetadataServiceTest,ClusterBrokerLifecycleTest test`; RED.
- [ ] Wire root lock/store/lifecycle/partition manager/listener and shutdown ownership. Listener can serve bootstrap/status while fenced, data admission stays closed. No local topic append; root lock outlives workers.

```java
return control.request(new QuorumProtocol.CreateTopic(name,count,(short)1,
    remainingMillis(deadline)),deadline).thenApply(this::committedCreateResult);
```

Add v2 CreateTopic constructor(name,count,RF,timeoutMs), old constructor remains legacy RF1 fixture. private remainingMillis enforces single deadline; private committedCreateResult maps typed reply preserving UNKNOWN across redirects. Capture all possibly admitted attempts, later NOT_LEADER cannot erase uncertainty.
- [ ] Remove production MetadataService.open wiring. Keep TopicCatalog/MetadataEventCodec for old metadata format. Move old local service fixture to `T/broker/support/LegacyMetadataFixture.java` only if a test needs it; no runtime standalone flag. Port broker tests to injected BrokerMetadata or genuine cluster. Port old Broker.start callers in integration/example to a compiling explicit config overload; acceptance migration completed Task15/16.
- [ ] Run `mvn -Dtest=ClusterMetadataServiceTest,ClusterBrokerLifecycleTest,BrokerLifecycleTest,BrokerIsolationTest,RequestDispatcherTest,MetadataServiceTest,MetadataRecoveryTest test`; GREEN. Commit `feat(broker): use quorum metadata and forward topic commands`.

## Task 13: Data v2 và checked I/O

**Files:** Create `M/protocol/ClusterProtocol.java`; modify `M/protocol/Protocol.java`, `M/protocol/ProtocolCodec.java`, `M/protocol/ErrorCode.java`, `M/protocol/ProtocolLimits.java`, `M/broker/RequestDispatcher.java`, `M/broker/PartitionRuntime.java`, `M/broker/FetchPlanner.java`, `M/broker/FetchCoordinator.java`, `M/transport/netty/NettyServerTransport.java`; create `T/protocol/ClusterProtocolCodecTest.java`, `T/broker/ClusterAdmissionGateTest.java`, `T/broker/ClusterFetchBoundaryTest.java`, `R/protocol/cluster-v2-vectors.txt`, `docs/protocol-v2.md`.

**Interfaces:** DTOs §2.3. Runtime overload `produce(Batch,AckMode,long,BooleanSupplier stillAllowed): CompletableFuture<Protocol.ProduceResult>`; analogous guarded read. Legacy local runtime test overload delegates true, never reachable from cluster admission without guard. Dispatcher adds explicit outcome before/after append. Frame version retained in RequestContext/server dispatch.

- [ ] RED outcomes/vectors:

```java
@Test void unknownIsNotRetrySafe() {
    var tp = new Protocol.TopicPartition(new UUID(0,7),0);
    var r = new ClusterProtocol.ProduceResult(tp,
        new Protocol.Error(ErrorCode.REQUEST_TIMED_OUT,"deadline"),
        ClusterProtocol.Outcome.UNKNOWN,-1,-1);
    assertFalse(r.retrySafe());
}
```

retrySafe checks REJECTED only; client additionally checks retryable code/deadline. Constructor rejects invalid error/outcome combinations.
- [ ] Run `mvn -Dtest=ClusterProtocolCodecTest,ClusterAdmissionGateTest,ClusterFetchBoundaryTest test`; RED.
- [ ] Implement exact v2 encoding/preflight; v1 bytes unchanged. Check cluster/route IDs/epochs/gate/readiness before schedule and again on ordered worker before I/O. Track internal appendStarted to classify uncertain failures; do not infer outcome merely from ErrorCode.

```java
if (!stillAllowed.getAsBoolean()) return rejectedBeforeAppend();
appendStarted = true;
AppendResult appended = store.append(batch.records());
// Failure after appendStarted is UNKNOWN even if no append offsets were returned.
```

Private per-operation state/mapper in runtime; callback carries outcome to dispatcher without global mutable flag. No global lock held across disk I/O.
- [ ] Wake long-poll on fencing, not heartbeat timeout. Capture start/LEO/HW and batches in same serialized view; enforce read end<=HW. Preserve offset filtering and oversized-first-batch flag across planner/runtime.
- [ ] Tests gate flips queued vs executing, FLUSHED waiter fence, delayed completion, partial multi-partition success, HW same-view consistency, malformed outcome, exact budgets. Run `mvn -Dtest=ClusterProtocolCodecTest,ClusterAdmissionGateTest,ClusterFetchBoundaryTest,ProtocolCodecTest,ProduceDurabilityTest,LongPollTest,FetchPlannerTest,NettyServerTransportTest test`; GREEN.
- [ ] Update sealed switches in every caller so tree compiles. Commit `feat(protocol): route cluster requests with explicit outcomes`. Checkpoint B: run the focused broker/protocol/core suites from Tasks10–13. Existing end-to-end v1 examples are migrated in Tasks14–16; record them as outstanding rather than disabling their assertions or claiming full-suite green.

## Task 14: ClusterClient routing, bounded fan-out và retry

**Files:** Create `M/client/RequestClient.java`, `M/client/ClusterClient.java`, `M/client/ClusterClientConfig.java`; modify `M/client/BrokerClient.java`, `M/client/ClientConfig.java`, `M/client/Producer.java`, `M/client/Consumer.java`, `M/client/ClientException.java`, `M/transport/netty/NettyClientTransport.java`; create `T/client/ClusterClientRoutingTest.java`, `T/client/ClusterClientRetryTest.java`, `T/client/ClusterFetchBudgetTest.java`; migrate `T/client/ProducerTest.java`, `T/client/ConsumerTest.java`, `T/client/NettyClientLoopbackTest.java`.

**Interfaces:** `RequestClient.request(Protocol.Request,long deadlineNanos): CompletableFuture<Protocol.Response>`. BrokerClient implements it with overload `request(Protocol.Request)` keeping existing default deadline for v1 tests. `ClusterClientConfig(List<InetSocketAddress> bootstrap,UUID expectedClusterId,int maxConnections,Duration operationTimeout,Duration retryBackoff,long queuedBytes,int maxInFlight)`; null expectedClusterId means pin first valid bootstrap cluster. `ClusterClient(ClusterClientConfig,ClientConfig,Function<InetSocketAddress,ClientTransport>,DeadlineScheduler)` implements RequestClient/AutoCloseable; `refresh(long): CompletableFuture<ClusterProtocol.MetadataReply>`. Producer/Consumer depend RequestClient, keep per-partition ordering and manual consumer offsets.

- [ ] RED routing/retry tests use new `T/client/ClusterClientHarness.java`: real ClusterClient + ManualScheduler, map of LoopbackTransport per address, inspect encoded request lists. APIs `client()`, `requests(int brokerId)`, `replyMetadata(Map<Integer,List<Integer>> ownership)`, `failAfterSend(int brokerId)`, `runDue()`, `close()`. Fixed clusterUUID(0,1), topicUUID(0,7), broker endpoints127.0.0.1:19091–19093, epochs1, partition ownership given by test.

```java
@Test void lostProduceResponseIsNotRetried() {
    try (var h = new ClusterClientHarness()) {
        h.replyMetadata(Map.of(1,List.of(0),2,List.of(1)));
        var result = h.client().request(h.produceToPartition(0),30_000_000_000L);
        h.failAfterSend(1);
        h.runDue();
        assertTrue(result.isCompletedExceptionally());
        assertEquals(1,h.requests(1).stream().filter(ClusterProtocol.Produce.class::isInstance).count());
    }
}
```

Harness `produceToPartition(int)` returns ClusterProtocol.Produce with one record `new LogRecord(0,null,new byte[]{1},List.of())`, RF1 known route, APPENDED. Replies use real codecs; don't fake retry logic in harness.
- [ ] Run `mvn -Dtest=ClusterClientRoutingTest,ClusterClientRetryTest,ClusterFetchBudgetTest test`; RED.
- [ ] Implement cluster pin, metadata cache keyed clusterId/topicUUID with per-response appliedOffset guard; single in-flight refresh. One connection per broker endpoint generation, max32, lazy connect, close old endpoint, release reservations once. Limit bootstrap live sockets within same cap, evict idle only, no unbounded pool.
- [ ] Split Produce by destination, retain request input order when merge; retry only explicit REJECTED retryable entries or NOT_SENT transport cases. Deadline is original absolute clock deadline across queue, metadata, reconnect, backoff. Preserve successful results; do not turn partial UNKNOWN into all-or-nothing exception that erases successes.

```java
boolean mayRetry = result.outcome() == ClusterProtocol.Outcome.REJECTED
    && retryableCode(result.error().code()) && clock.nanoTime() < deadline;
```

Private retryableCode allows NOT_PARTITION_LEADER/STALE_PARTITION_EPOCH/STALE_BROKER_EPOCH/FENCED_BROKER/PARTITION_UNAVAILABLE/OVERLOADED; never INVALID_REQUEST/STORAGE_ERROR/REQUEST_TIMED_OUT by code alone. For exception UNKNOWN complete affected entries UNKNOWN. Produce lane one batch in-flight through retries, later batch cannot overtake it.
- [ ] Fetch fan-out: allocate global maxBytes/per-partition maxBytes; exactly one designated subrequest may request oversized first batch, others flagfalse. If exception used, stop scheduling further data; existing results still fit total response absolute8MiB cap. Whole-operation minBytes/long-poll with original deadline: aggregate short polls, avoid per-broker minBytes multiplication. Fetch retry same offset; Consumer alone advances user-visible offsets explicitly.
- [ ] Tests bootstrap failover, foreign cluster, unknown topic refresh, UUID same name mismatch, single-flight refresh, endpoint generation, close pending, max connections/bytes, partial success, expired deadline not reset, oversized response across three brokers, Produce order. Run `mvn -Dtest=ClusterClientRoutingTest,ClusterClientRetryTest,ClusterFetchBudgetTest,ProducerTest,ConsumerTest,BrokerClientTest,ClientOutcomeTest,TransportConformanceTest test`; GREEN.
- [ ] Commit `feat(client): route cluster traffic with bounded retries`. Checkpoint C: run Tasks10–14 focused suites including existing client/conformance regressions. Full end-to-end suite runs after legacy launch fixtures are migrated in Tasks15–16.

## Task 15: Deterministic cluster faults và six-process acceptance

**Files:** Create `T/integration/support/Phase4Processes.java`, `T/integration/support/ClusterProcessMain.java`, `T/integration/ClusterRoutingTest.java`, `T/integration/ClusterControllerLossTest.java`, `T/integration/ClusterBrokerRestartTest.java`, `T/integration/ClusterObserverCatchupTest.java`, `T/integration/ClusterProvisioningCrashTest.java`, `T/broker/cluster/ClusterFaultCampaignTest.java`; extend `T/controller/support/ControllerFaultProxy.java`, `T/controller/support/ThreeControllerProcesses.java`; migrate `T/integration/BrokerNetworkFaultTest.java`, `T/integration/BrokerCrashTest.java`, `T/integration/BrokerBackpressureTest.java`, `T/integration/BrokerExampleTest.java`, `T/integration/BrokerCliSmokeTest.java`; create `R/cluster/fault-seeds.txt`.

**Interfaces:** `Phase4Processes.start(Path root): Phase4Processes` formats3 controllers/3 brokers and starts actual production nodes; `client(): ClusterClient`, `createTopic(String,int): UUID`, `produceFlushed(UUID,int,byte[]): long`, `fetchValues(UUID,int): List<byte[]>`, `disconnectControllersFromBrokers()`, `heal()`, `killBroker(int)`, `restartBroker(int)`, `killController(int)`, `awaitRunning(int,Duration)`, `awaitObserverSnapshot(int,Duration)`, `close()`. IDs broker1–3; first partition goes broker1 by tie-break. Proxy disconnects only control links, never data ports. Child-process logs under target/cluster-process-logs. `ClusterProcessMain` accepts config/role/data and calls production composition, not bespoke broker logic.

- [ ] RED test data plane survives lost controller connectivity:

```java
@Test void runningBrokerKeepsServingWithoutControllers(@TempDir Path root) throws Exception {
    try (var cluster = Phase4Processes.start(root)) {
        UUID topic = cluster.createTopic("orders",6);
        cluster.produceFlushed(topic,0,new byte[]{1});
        cluster.disconnectControllersFromBrokers();
        cluster.awaitControlIsolation(Duration.ofSeconds(20));
        cluster.produceFlushed(topic,0,new byte[]{2});
        assertEquals(2,cluster.fetchValues(topic,0).size());
        cluster.killBroker(1);
        cluster.restartBroker(1);
        assertFalse(cluster.becomesRunning(1,Duration.ofSeconds(12)));
    }
}
```

Fixture `awaitControlIsolation(Duration)` repeatedly proves proxies block all control sockets and observes heartbeat retries for stated duration, not a blind sleep; `becomesRunning(int,Duration)` polls production status in bounded deadline and returns false on expiry. Do not require refresh through controller for existing cached route in isolation test.
- [ ] Run targeted test on Linux/ext4; RED only behavior/assertion failure or not-yet-defined fixture. On Windows explicit platform guard for strict process suites; don't describe skipped test as pass.
- [ ] Implement real process fixture with bounded startup/cleanup, root locations validated, port reservation/retry, no leaked JVM. Reuse ThreeControllerProcesses and proxy framing rather than bypass consensus.

```java
Process process = new ProcessBuilder(javaExecutable, "-cp", classpath,
    "vn.huyqt.logbroker.integration.support.ClusterProcessMain",
    "--role", role, "--config", configPath.toString(), "--data", dataPath.toString())
    .redirectErrorStream(true).redirectOutput(logPath.toFile()).start();
```

Fixture computes javaExecutable from java.home and test classpath, passes argument list, tracks every process before awaiting ready; finally destroy, await, force kill if necessary and retain logs.
- [ ] Add cases controller majority lost (kill2 voters) leaves warmed RF1 data serving, cannot create/register; isolated broker remains stale until committed fence arrives then stops, same-session catch-up/new unfence restores; whole-cluster restart preserves metadata and FLUSHED data. Compare UUID/epoch/offset/content, not only socket readiness.
- [ ] Snapshot catch-up must prove actual prefix deletion, nonzero snapshot end, new observer generation and identical broker/topic/partition image; pause broker past threshold, create enough metadata then heal. Faults during download/install and provisioning force boundaries use FaultFiles powerLoss separately from process-kill tests.
- [ ] Extend deterministic fixture `T/broker/cluster/ClusterFaultHarness.java` with actual3 quorum state machines + broker lifecycle/observer + FakeDisk/network queue. API `ClusterFaultHarness(long seed)`, `run(int steps)`, `healAndDrain()`, `assertInvariants()`, `close()`. Invariants: committed-only apply, no partial topic, no stale callback reopen, no observer majority, no retry UNKNOWN, no missing-log recreation, bounded budgets. Track expected committed content/session lineage with independent model, not by echoing production image.

```java
@Test void deterministicFaultCampaign() throws Exception {
    for (long seed : List.of(1L,7L,42L,20261003L)) {
        try (var h = new ClusterFaultHarness(seed)) {
            h.run(2000);
            h.healAndDrain();
            h.assertInvariants();
        }
    }
}
```

Implement `-Dcluster.seedCount=100` to extend seed list using `index*7919L`; include deliberately broken harness fixtures proving oracle catches duplicate retry, uncommitted apply and stale unfence. Keep checked seeds resource sorted and replayable.
- [ ] Run `mvn -Dtest=ClusterRoutingTest,ClusterControllerLossTest,ClusterBrokerRestartTest,ClusterObserverCatchupTest,ClusterProvisioningCrashTest,ClusterFaultCampaignTest test` on ext4; GREEN. Migrate old integration tests to this cluster fixture or injected core fixture, preserve assertions and no production standalone fallback.
- [ ] Commit `test(cluster): verify routing fencing and recovery under faults`.

## Task 16: Operator commands, examples, status và final verification

**Files:** Modify `M/broker/BrokerMain.java`, `M/controller/ControllerMain.java`, `M/controller/client/ControllerCli.java`, `M/example/ClientExample.java`, `README.md`, `docs/broker-configuration.md`, `docs/controller-configuration.md`, `docs/controller-operation.md`, `docs/controller-verification.md`; create `docs/cluster-configuration.md`, `docs/cluster-operation.md`, `docs/cluster-verification.md`, `config/cluster/controller-1.properties`, `config/cluster/controller-2.properties`, `config/cluster/controller-3.properties`, `config/cluster/broker-1.properties`, `config/cluster/broker-2.properties`, `config/cluster/broker-3.properties`, `scripts/cluster-demo.sh`; extend `T/integration/BrokerCliSmokeTest.java`, `T/broker/BrokerMainTest.java`, `T/controller/ControllerCliTest.java`.

**Interfaces:** Broker format CLI Task7; production startup `BrokerMain --config path --data root`. ClientExample accepts comma-separated bootstrap `host:port` list and topic; starts ClusterClient/Producer/Consumer with cleanup. Add broker `status` via local status logging/diagnostic snapshot API `Broker.status(): BrokerStatus` (new M/broker/cluster/BrokerStatus.java record) containing identity/session/lifecycle, controller hint, heartbeat age, observer offsets/snapshot generation, partition state counts and budget usage. No external metrics dependency. CLI controller remains same command vocabulary but v2 metadata view includes assignments/broker sessions.

- [ ] RED CLI smoke verifies unformatted root rejected, explicit format then cluster boot/client success, duplicate brokerId rejected; command help names new required config. No actual controller storage format migration promised.

```java
@Test void cliExampleUsesBootstrapBrokerForRemotePartitions() throws Exception {
    try (var cluster = Phase4Processes.start(tempRoot)) {
        var output = cluster.runClientExample("127.0.0.1:" + cluster.brokerPort(1), "demo");
        assertTrue(output.contains("SUCCESS"));
        assertTrue(output.contains("brokers=3"));
    }
}
```

Extend Phase4Processes `brokerPort(int)`, `runClientExample(String,String): String` uses ProcessBuilder invokes production example and requires exit0. Example creates6 partitions and explicitly exercises each, so brokers=3 is computed from metadata actually used, not literal print.
- [ ] Run `mvn -Dtest=BrokerCliSmokeTest,BrokerMainTest,ControllerCliTest test`; RED before updated CLI/example.
- [ ] Implement script with explicit cluster UUID, fresh roots,3 controllers/3 brokers and trap cleanup. Do not recursively delete any existing user directory. Default data under target for ordinary launch; strict verification script accepts ext4 data root and probes provider. Config samples match documented bounds/advertised ports, no hidden standalone fallback.

```bash
mvn verify dependency:copy-dependencies
java -cp 'target/classes:target/dependency/*' vn.huyqt.logbroker.broker.BrokerMain format --config config/cluster/broker-1.properties --data target/cluster/broker-1
java -cp 'target/classes:target/dependency/*' vn.huyqt.logbroker.broker.BrokerMain --config config/cluster/broker-1.properties --data target/cluster/broker-1
```

Document all3 controller format/start commands and broker configs; explain `;` Windows classpath, strict acceptance requires WSL/ext4 checkout not /mnt/d. Document NOT_SENT/UNKNOWN, RF1 loss, known-fence vs disconnection, metadata local consistency, missing-log failure, inventory/journal cap and snapshot limits.
- [ ] Add status structured log at lifecycle transitions/failure/periodic bounded cadence, not each record. Include failed partition paths without payload or secret contents. Exercise queue/budget saturation status in unit fixture.
- [ ] Run focused CLI suites GREEN, then commands below on Java21/Maven3.9.x Linux/ext4. Record exact results/HEAD/platform/skips/fault seeds/durability probe in docs/cluster-verification.md; no fabricated expected test count.

```bash
mvn clean verify dependency:copy-dependencies
mvn -Dtest=ClusterFaultCampaignTest -Dcluster.seedCount=100 test
mvn -Dtest=QuorumFaultCampaignTest,LinearizabilityHistoryTest -Dquorum.seedCount=100 test
mvn -Dtest=ClusterRoutingTest,ClusterControllerLossTest,ClusterBrokerRestartTest,ClusterObserverCatchupTest,ClusterProvisioningCrashTest,BrokerCliSmokeTest test
```

Expected: exit0, zero failures/errors; strict campaigns/process suites zero skips. Windows run may skip explicit strict process tests but cannot satisfy acceptance. Inspect Surefire XML and logs for all expected classes; exit0 with missing tests is not evidence.
- [ ] Update spec/roadmap status only once acceptance actually passes. Plan completion now does not claim code implemented. Commit `docs(cluster): document phase 4 operations and verification`.

## 3. Spec coverage và handoff gates

| Spec requirement | Tasks | Evidence |
|---|---|---|
| Scope/authority/RF1 and no migration (§1–3) |4,7,10,12,15|startup/restart/unknown outcome tests|
| Identity/versions (§4) |1–4,7,8|format + version range + bootstrap replay|
| Lifecycle/heartbeat/fencing (§5–6) |4,5,9,10,13,15|timeout continues, committed fence stops, stale reply ignored|
| Atomic assignment/epochs (§7) |1,2,4,13|batch apply/replay/conflict/pending load|
| Observer/snapshot (§8–9) |2,3,6,8,9,15|committed boundary + generation crash + prefix deleted catch-up|
| Inventory/provisioning (§10) |7,11,13,15|intent/complete crash and missing-log cases|
| Wire/routing/retry (§11) |3,12,13,14|vectors, multibroker fan-out, UNKNOWN no retry|
| Bounds/observability (§12) |1,3,5,6,7,14,16|preflight/pressure/status tests|
| Acceptance/docs (§13–16) |15,16|Linux ext4 process/fault/CLI evidence|

- [ ] Checkpoint A: controller cluster metadata/control endpoint, baseline consensus unchanged.
- [ ] Checkpoint B: cluster-only broker composes and gates local I/O with durable inventory.
- [ ] Checkpoint C: client routes real wire v2, retry/budget semantics tested.
- [ ] Final: full/fault/process/CLI evidence; no uncommitted user changes included accidentally; review diff against spec; no implementation success claim based on this document.

## 4. Plan self-review record

Plan writer verifies spec coverage table, all Create/Modify paths, existing API signatures in snippets, v1/v2 numeric compatibility, session outcomes, generation/publication order, task dependency graph and explicit RED/GREEN commands. Test files in this plan are proposed; tests have not been executed by the planning turn. Execution must verify actual behavior and update checkboxes/evidence as work completes.

Execution choice after plan review: subagent-driven per task with review gates, or inline task-by-task with checkpoints. Do not begin source implementation during planning. Any discovered change to safety/availability promises goes back to design review; implementation details within these contracts are handled directly.

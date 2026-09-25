# Single Broker and Java Client — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây broker đơn có topic/partition, protocol riêng và Java client, trên storage Phase 1 đã có.

**Architecture:** Netty chỉ xử lý transport; core dùng message Java và thực thi tuần tự mỗi partition trên pool giới hạn. Local metadata log lưu catalog; Produce xác nhận APPENDED/FLUSHED; Fetch đọc đến log end và hỗ trợ long polling. Batching, correlation và deadline ở client độc lập với storage.

**Tech Stack:** Java 21, Maven single module, JUnit Jupiter 5.11.4 hiện có; Netty 4.2.18.Final dùng NIO transport trên Windows. Không Spring, Kafka client, mocking framework hoặc native transport.

**Spec:** [Phase 2 đã duyệt](../specs/2026-09-25-broker-phase-2-design.md). Đọc cả spec và plan trước thực hiện.

## Global Constraints

- Namespace: `vn.huyqt.logbroker`.
- APPENDED thành công sau khi PartitionLog.append hoàn tất. FLUSHED thành công khi durableEndOffset >= nextOffset của batch.
- durableEndOffset không phải replication high watermark.
- Không atomic giữa các partition.
- Không tự retry Produce, kể cả reconnect.
- Core không nhận Channel, ByteBuf hay kiểu Netty khác.
- Netty event loop không thực hiện storage I/O hoặc chờ future.
- Chưa có compression, attributes mở rộng hoặc zero-copy.
- Không dùng unbounded executor hoặc queue làm mặc định.
- Process-kill test không chứng minh an toàn trước mất điện.
- Toàn bộ defaults ở spec mục 11 là bắt buộc, gồm 10 ms/1 MiB flush, frame 8 MiB, Fetch budget 4 MiB, 64 partition/request, 10.000 record/batch và các cap tài nguyên.
- Không thay đổi storage v1 hoặc thêm KRaft, replication, group coordination, retention, TLS/auth trong phase này.

## Bối cảnh, phạm vi và thứ tự

Root: `D:/User2/distributed-system-labs/java-log-broker`. Storage Phase 1 đã có code và tests, không dùng mô tả repository rỗng trong plan Phase 1 làm trạng thái hiện tại. Không sửa repository `../kafka`. Chỉ tạo worktree khi bắt đầu thực thi, theo skill execution được chọn.

Giữ một plan tích hợp vì protocol, broker và client cùng thực hiện một hợp đồng end-to-end; các task dưới đây có test riêng và checkpoint để review độc lập. Task 1–3 tạo codec; 4–8 tạo broker core; 9–11 nối transport/client; 12–14 hoàn thiện lifecycle và fault tests. Không triển khai code khi đang chỉ viết plan.

Trước Task 1 chạy `mvn clean verify` làm baseline. Dependency download failure không được tính là test đỏ. Mỗi task chạy targeted tests đỏ → thay đổi nhỏ → xanh; chia từng nhóm case thành vòng riêng, không viết toàn bộ subsystem rồi mới chạy. Snippet là test/kernel cụ thể; acceptance cases bổ sung trong cùng task cũng phải triển khai. Mỗi commit chỉ stage danh sách file của task, không dùng `git add .` khi có thay đổi khác.

## Bản đồ file và quy ước đường dẫn

Các prefix dưới đây là phép thay thế đường dẫn chính xác, không phải package mới:

- `M/` = `src/main/java/vn/huyqt/logbroker/`
- `T/` = `src/test/java/vn/huyqt/logbroker/`
- `R/` = `src/test/resources/`

| Nhóm file | Vai trò |
| --- | --- |
| M/storage/RecordPayloadCodec.java | Encoding record dùng chung; BatchCodec giữ header/CRC storage |
| M/protocol/Protocol.java, ErrorCode.java, ProtocolLimits.java | Immutable messages, mã lỗi, hard bounds |
| M/protocol/WireBatchCodec.java, ProtocolCodec.java, ProtocolException.java | Codec thuần Java, không Netty |
| M/broker/BrokerConfig.java, ResourceBudget.java, DeadlineScheduler.java | Config, lease admission, monotonic timer |
| M/broker/PartitionExecutor.java | Bounded queues, mỗi partition một runner |
| M/broker/metadata/TopicCatalog.java, MetadataEventCodec.java, MetadataService.java | Metadata replay/write/apply |
| M/broker/PartitionStore.java, FilePartitionStore.java, PartitionRegistry.java | Ranh giới disk nhỏ để fault injection |
| M/broker/PartitionRuntime.java, FlushCoordinator.java | Append, dirty ledger, waiter, failure isolation |
| M/broker/FetchPlanner.java, FetchCoordinator.java | Budget và long poll |
| M/broker/RequestContext.java, RequestDispatcher.java | Admission, deadline, partial result |
| M/transport/ServerTransport.java, ClientTransport.java | Ranh giới lifecycle/send/disconnect |
| M/transport/netty/BoundedFrameDecoder.java, NettyServerTransport.java, NettyClientTransport.java | Frame admission và TCP |
| M/client/ClientConfig.java, BrokerClient.java, ClientException.java | Correlation và API request |
| M/client/Producer.java, Partitioner.java, BatchAccumulator.java, Consumer.java | Record API, batching và explicit fetch |
| M/broker/Broker.java, BrokerMain.java; M/example/ClientExample.java | Composition, lifecycle và demo |
| T/support/ManualScheduler.java, FakePartitionStore.java, BrokerHarness.java, LoopbackTransport.java | Clock, controlled I/O, integration rig |
| docs/protocol-v1.md, docs/broker-configuration.md | Wire contract và cách vận hành |

Không tạo class trống cho toàn bộ bảng ngay từ đầu. Tạo file ở task sử dụng đầu tiên. Metadata classes có thể dùng nested immutable records cho TopicCreated/TopicState; Protocol dùng nested records để nhóm DTO có cùng vòng đời, không tạo một file cho từng trường.

## Hợp đồng dùng chung

`Protocol` chứa các record/enum sau, mọi collection copy bất biến; record payload sử dụng `storage.LogRecord` bất biến có sẵn:

```java
record TopicPartition(UUID topicId, int partition) {}
enum AckMode { APPENDED, FLUSHED }
record Batch(List<LogRecord> records) {}
record FetchBatch(long baseOffset, Batch batch) {}
record ProduceEntry(TopicPartition partition, Batch batch) {}
record FetchEntry(TopicPartition partition, long offset, int maxBytes) {}
record Error(ErrorCode code, String message) {}
record ProduceResult(TopicPartition partition, Error error,
                     long firstOffset, long nextOffset) {}
record FetchResult(TopicPartition partition, Error error, long logStartOffset,
                   long logEndOffset, List<FetchBatch> batches) {}
record PartitionInfo(int partition, Error error) {}
record TopicInfo(String name, UUID id, List<PartitionInfo> partitions) {}
sealed interface Request {}
sealed interface Response {}
record CreateTopic(String name, int partitions) implements Request {}
record Metadata(List<String> names) implements Request {}
record Produce(AckMode ack, int timeoutMs, List<ProduceEntry> entries) implements Request {}
record Fetch(int maxBytes, int minBytes, int maxWaitMs,
             List<FetchEntry> entries) implements Request {}
record CreateTopicReply(Error error, UUID topicId) implements Response {}
record MetadataReply(Error error, String host, int port,
                     List<TopicInfo> topics) implements Response {}
record ProduceReply(Error error, List<ProduceResult> results) implements Response {}
record FetchReply(Error error, List<FetchResult> results) implements Response {}
record Failure(Error error) implements Response {}
record RequestFrame(short operation, short version, long requestId, Request body) {}
record ResponseFrame(short operation, short version, long requestId, Response body) {}
```

Unknown topic trong Metadata query làm request trả UNKNOWN_TOPIC (không trả topic ID giả). Metadata names rỗng là list-all; giới hạn names=128 và không trùng. UUID zero dành riêng cho CreateTopicReply thất bại trước khi có ID; topic ID được cấp luôn khác zero. Produce/Fetch unknown topic vẫn có kết quả riêng theo entry.

`PartitionStore extends AutoCloseable` có `AppendResult append(List<LogRecord>) throws IOException`, `List<RecordBatch> read(long,int) throws IOException`, `long flush() throws IOException`, `long logStartOffset()`, `long logEndOffset()`, `long durableEndOffset()`, `void close() throws IOException`. FilePartitionStore delegate trực tiếp PartitionLog; không export truncate. Factory `PartitionStore open(Path, LogConfig) throws IOException` là nested functional interface của PartitionStore.

`DeadlineScheduler extends AutoCloseable`: `long nanoTime()`, `Ticket schedule(long deadlineNanos,Runnable action)`, `void close()`; nested `Ticket.cancel()` trả boolean. Production dùng một scheduled thread, remove-on-cancel; số timer bị chặn bởi admitted contexts/partitions. ManualScheduler có `advance(Duration)` và `runDue()`; callback không làm disk I/O.

`BrokerHarness` được hoàn thiện dần cùng core, nằm trong test support: `static BrokerHarness open(Path, BrokerConfig, ManualScheduler)`, `CompletableFuture<Response> request(Request)`, `void drain()` (chờ các tác vụ sẵn sàng, không chờ timer tương lai), `TopicPartition create(String,int)` (trả partition 0), `FakePartitionStore store(TopicPartition)`, `void close()`. Harness dùng memory stores; test persistence dùng FilePartitionStore và thư mục thật riêng. Fake store có `int flushCalls()`, `void failNextFlush(IOException)`, `void blockAppend(CountDownLatch entered, CountDownLatch release)`; giữ nguyên offsets và marker như storage thật, không thay thế integration tests.

## Wire contract v1 — nguồn để viết docs/protocol-v1.md trong Task 2

Tất cả số big-endian, signed; UUID = hai int64 most/least-significant bits. Boolean/enum dùng int8 với giá trị hợp lệ liệt kê. String = int32 số byte UTF-8 rồi bytes, strict UTF-8, không nullable. Array = int32 count rồi các phần tử. Reject trailing bytes. Error = int16 code + string message, message tối đa 512 byte. NONE=0 dùng message rỗng.

Frame: int32 length (không gồm chính 4 byte đó), int16 operation, int16 version, int64 requestId, body. length trong [12, 8 MiB]; version=1; requestId không âm. Response luôn có Error cấp request đầu body; nếu error khác NONE body kết thúc tại đó, decoder trả Failure hoặc typed error khi phù hợp. Request error không đưa partial results; lỗi sau dispatch phải là per-entry.

| Operation | Request body, theo thứ tự | Response body sau Error=NONE |
| --- | --- | --- |
| 1 CreateTopic | string name, int32 partitionCount | UUID topicId |
| 2 Metadata | array<string> names | string host (max 255 byte), int32 port, array<TopicInfo> |
| 3 Produce | int8 ack (0 APPENDED, 1 FLUSHED), int32 timeoutMs, array<ProduceEntry> | array<ProduceResult> |
| 4 Fetch | int32 maxBytes, int32 minBytes, int32 maxWaitMs, array<FetchEntry> | array<FetchResult> |

TopicInfo = string name + UUID id + array<PartitionInfo>; PartitionInfo = int32 id + Error. ProduceEntry = UUID + int32 partition + wire batch. FetchEntry = UUID + int32 partition + int64 offset + int32 partitionMaxBytes. ProduceResult = UUID + int32 partition + Error + int64 firstOffset + int64 nextOffset; offsets=-1 khi lỗi. FetchResult = UUID + int32 partition + Error + int64 start + int64 end + array<FetchBatch>; start/end=-1 và batch array rỗng khi lỗi. FetchBatch = int64 baseOffset + wire batch.

Wire batch header: int16 version=1 tại 0, int32 totalLength tại 2, int32 recordCount tại 6, CRC32C tại 10, payload tại 14. CRC tính bytes [0,10) rồi [14,totalLength). Không có outer batch-length thừa. Payload như storage v1. Với cùng records: storage bytes = payload+30; Produce batch bytes=payload+14; Fetch batch bytes=payload+22. maxWireBatchBytes gồm 8-byte baseOffset; validate cả payload+30 <= storage cap và payload+22 <= wire cap khi Produce.

ErrorCode mapping: NONE=0, INVALID_REQUEST=1, UNSUPPORTED_OPERATION=2, UNSUPPORTED_VERSION=3, UNKNOWN_TOPIC=4, UNKNOWN_PARTITION=5, TOPIC_ALREADY_EXISTS=6, OFFSET_OUT_OF_RANGE=7, BATCH_TOO_LARGE=8, OVERLOADED=9, REQUEST_TIMED_OUT=10, PARTITION_UNAVAILABLE=11, STORAGE_ERROR=12, BROKER_SHUTTING_DOWN=13. Unknown numeric error ở client trở thành protocol exception, không success. Header hợp lệ nhưng unknown op/version trả Failure echo header; frame/envelope không đọc an toàn được thì đóng connection.

Phân loại validation: framing/body structure sai (length/count/UTF-8/CRC/version batch/duplicate partition) từ chối request trước dispatch; unknown topic/partition, offset out-of-range, admission và storage errors là per-entry. Batch hợp lệ nhưng vượt storage cap có thể trả BATCH_TOO_LARGE per-entry; không mutate entry đó. Frame vượt wire cap đóng connection trước decode.

## Task 1: Chia sẻ record payload codec mà giữ nguyên storage format

**Files:** tạo M/storage/RecordPayloadCodec.java; sửa M/storage/BatchCodec.java chỉ phần size/write/read payload; tạo T/storage/RecordPayloadCodecTest.java và R/storage/batch-v1.hex.

**Interfaces:** `public static int encodedSize(List<LogRecord>)`; `public static void write(ByteBuffer,List<LogRecord>)`; `public static List<LogRecord> read(ByteBuffer,int count) throws CorruptLogException`. Caller cấp bounded payload slice; read phải consume hết. Không áp 10.000 record vào recovery storage cũ; wire layer áp cap riêng.

- [ ] Ghi golden fixture từ format v1 độc lập (header và CRC tính tường minh), rồi viết test so sánh byte trước refactor và payload round-trip:
```java
var records = List.of(new LogRecord(7L, null, new byte[0], List.of()));
assertEquals(20, RecordPayloadCodec.encodedSize(records));
ByteBuffer payload = ByteBuffer.allocate(20);
RecordPayloadCodec.write(payload, records);
assertEquals(records, RecordPayloadCodec.read(payload.flip(), 1));
```
- [ ] Chạy `mvn -Dtest=RecordPayloadCodecTest test`, mong đợi compile fail do codec mới chưa tồn tại.
- [ ] Di chuyển size loop/UTF-8/nullable/header logic từ BatchCodec; giữ nguyên header validation, CRC positions và exception mapping. Kernel size dùng checked arithmetic:
```java
long size = 0;
for (LogRecord record : records) {
    size = Math.addExact(size, 20L);
    size = Math.addExact(size, record.key() == null ? 0 : record.key().length);
    size = Math.addExact(size, record.value() == null ? 0 : record.value().length);
    for (RecordHeader header : record.headers()) {
        size = Math.addExact(size, 8L + header.key().getBytes(StandardCharsets.UTF_8).length);
        size = Math.addExact(size, header.value() == null ? 0 : header.value().length);
    }
}
return Math.toIntExact(size);
```
Giữ strict UTF-8 encoder hiện có trong code thực thi để reject surrogate sai; kernel trên chỉ minh họa phép tính độ dài. Thêm malformed UTF-8, null/rỗng, duplicate headers, truncated payload và overflow tests.
- [ ] Chạy `mvn test`; golden fixture phải không đổi, mọi Phase 1 test qua.
- [ ] Stage ba file task và commit `refactor: share record payload codec without format changes`.

## Task 2: DTO và operation codecs có wire contract cố định

**Files:** tạo M/protocol/{Protocol,ErrorCode,ProtocolLimits,ProtocolException,WireBatchCodec,ProtocolCodec}.java; T/protocol/{WireBatchCodecTest,ProtocolCodecTest}.java; R/protocol/{create-topic-v1,produce-v1,fetch-v1}.hex; docs/protocol-v1.md.

**Interfaces:** `ProtocolLimits.defaults()`; `WireBatchCodec.encode(Batch,ProtocolLimits): byte[]`, `decode(byte[],ProtocolLimits): Batch`, `fetchSize(Batch): int`; `ProtocolCodec(ProtocolLimits)` với `encodeRequest(RequestFrame): byte[]`, `decodeRequest(byte[]): RequestFrame`, `encodeResponse(ResponseFrame): byte[]`, `decodeResponse(byte[]): ResponseFrame`. byte[] gồm prefix frame. ProtocolException chứa ErrorCode và là checked IOException. Oversize request body không được copy trước kiểm tra cap.

- [ ] Chép wire contract phía trên vào docs/protocol-v1.md. Viết round-trip và literal CreateTopic fixture (`x`, 1 partition, id=7; length=21):
```java
var frame = new RequestFrame((short)1, (short)1, 7, new CreateTopic("x", 1));
byte[] expected = HexFormat.of().parseHex(
    "00000015000100010000000000000007000000017800000001");
var codec = new ProtocolCodec(ProtocolLimits.defaults());
assertArrayEquals(expected, codec.encodeRequest(frame));
assertEquals(frame, codec.decodeRequest(expected));
```
- [ ] Chạy `mvn -Dtest=WireBatchCodecTest,ProtocolCodecTest test` để thấy đỏ.
- [ ] Implement DTO defensive copies, enum numeric mapping, bounded size preflight rồi ByteBuffer encode; decoder validate counts theo remaining bytes trước allocation. CRC kernel:
```java
CRC32C crc = new CRC32C();
crc.update(bytes, 0, 10);
crc.update(bytes, 14, bytes.length - 14);
buffer.putInt(10, (int) crc.getValue());
```
- [ ] Thêm literal golden Produce/Fetch fixtures với null/rỗng/header; invalid length/count/CRC/op/version/duplicate partitions, response error không body, leftover bytes; run targeted tests xanh. Không dùng encode rồi decode làm golden duy nhất.
- [ ] Commit `feat: define versioned broker wire protocol`.

## Task 3: Config, budget leases và monotonic deadlines

**Files:** tạo M/broker/{BrokerConfig,ResourceBudget,DeadlineScheduler}.java; M/client/ClientConfig.java; T/support/ManualScheduler.java; T/broker/{BrokerConfigTest,ResourceBudgetTest,DeadlineSchedulerTest}.java.

**Interfaces:** `BrokerConfig.defaults(Path)`, immutable `withPort(int)` và `withFlushInterval(Duration)` cho tests; `ClientConfig.defaults(InetSocketAddress)`; `ResourceBudget(long capacity)`, `Optional<Lease> reserve(long)`, `long used()`; Lease implements AutoCloseable, close idempotent. Timer interface theo hợp đồng chung; production factory `DeadlineScheduler.system()`.

- [ ] Test double release/overflow, invalid cap combinations và cancelled timer:
```java
var budget = new ResourceBudget(10);
var lease = budget.reserve(7).orElseThrow();
assertTrue(budget.reserve(4).isEmpty());
lease.close(); lease.close();
assertEquals(0, budget.used());
```
- [ ] Chạy `mvn -Dtest=BrokerConfigTest,ResourceBudgetTest,DeadlineSchedulerTest test` đỏ.
- [ ] Dùng synchronized reserve: reject negative; `amount > capacity-used` thay vì cộng có thể overflow; lease CAS trả quota đúng một lần. Copy mọi default spec §11; validate `max(frameMetadata+fetchBudget,frameMetadata+maxWireBatch) <= frameCap`, topic/name/count caps, positive threads, segment >= storageBatch. ScheduledThreadPoolExecutor(1), remove-on-cancel, admitted count bounds, monotonic deadlines.
```java
if (amount < 0 || amount > capacity - used) return Optional.empty();
used += amount;
return Optional.of(new Lease(this, amount));
```
- [ ] Test defaults bằng giá trị literal từ spec, manual advance không dùng sleep; targeted suite xanh.
- [ ] Commit `feat: bound broker resources and deadlines`.

## Task 4: Metadata log và partition registry có recovery

**Files:** tạo M/broker/{PartitionStore,FilePartitionStore,PartitionRegistry}.java; M/broker/metadata/{TopicCatalog,MetadataEventCodec,MetadataService}.java; T/broker/metadata/{MetadataServiceTest,MetadataRecoveryTest}.java; T/support/FakePartitionStore.java.

**Interfaces:** TopicCatalog nested `TopicCreated(UUID id,String name,int partitions)`, `void apply(TopicCreated)`, `List<TopicInfo> snapshot()`. `MetadataService.open(Path,BrokerConfig,PartitionRegistry)` throws IOException; `CompletableFuture<CreateTopicReply> create(String,int)`; `MetadataReply metadata(List<String>)`; `close()`. `PartitionRegistry(BrokerConfig,PartitionStore.Factory)`, `void initialize(TopicInfo)`, `PartitionStore require(TopicPartition)`, `void markFailed(TopicPartition,Throwable)`, `close()`.

- [ ] Test same-name idempotence và reopen bằng FilePartitionStore:
```java
UUID id;
try (var registry = new PartitionRegistry(config, FilePartitionStore::open);
     var metadata = MetadataService.open(dir, config, registry)) {
    id = metadata.create("orders", 2).get(5, TimeUnit.SECONDS).topicId();
    assertEquals(id, metadata.create("orders", 2).get().topicId());
    assertEquals(ErrorCode.TOPIC_ALREADY_EXISTS,
        metadata.create("orders", 3).get().error().code());
}
```
Trong cùng test mở lại service và kiểm tra ID, 2 partition, end offset 0. `dir/config` do @TempDir và BrokerConfig.defaults(dir).
- [ ] Chạy `mvn -Dtest=MetadataServiceTest,MetadataRecoveryTest test` đỏ.
- [ ] TopicCreated value encoding: int16 eventVersion=1, UUID, string name, int32 count; mỗi storage batch một record timestamp=0/key=null/headers rỗng. Replay require contiguous events, hợp lệ name/count, unique UUID/name; duplicate exact event có thể apply idempotently, conflicting event fail startup. Serialize create trên một bounded worker. Append → flush → catalog.apply → registry.initialize; namespace path bằng UUID. Gọi fatal callback khi metadata write/force lỗi.
```java
metadataLog.append(List.of(new LogRecord(0, null, eventBytes, List.of())));
metadataLog.flush();
catalog.apply(event);
```
MetadataService constructor/open overload nội bộ nhận `Consumer<Throwable> fatalHandler`; public default log lỗi/đánh dấu service failed, Broker ở Task 12 cung cấp shutdown callback. Initialize lỗi data phải báo unavailable, không rollback catalog.
- [ ] Test crash boundary bằng tạo metadata event đã flush nhưng thiếu directory rồi reopen; corrupt metadata CRC fail; data corruption chỉ partition lỗi; concurrent same-name requests; catalog caps trước append; orphan directory không thu nhận/xóa. Targeted tests xanh.
- [ ] Commit `feat: persist local topic metadata and recover partitions`.

## Task 5: Fair partition queues và control priority

**Files:** tạo M/broker/PartitionExecutor.java; T/broker/PartitionExecutorTest.java.

**Interfaces:** `PartitionExecutor(int workers,int partitionLimit,int queuedTaskLimit)`, `<V> CompletableFuture<V> submit(TopicPartition,Callable<V>)`, `void control(TopicPartition,Runnable)`, `CompletableFuture<Void> drain()`, `void close()`. Rejection là RejectedExecutionException trước chạy. Deadline checks do caller trong Callable, không giấu mutation.

- [ ] Latch-test: task 1 giữ partition A, task 2 A không chạy, task B vẫn hoàn thành; enqueue quá cap bị từ chối, control vẫn chạy sau task 1 và trước task 2.
```java
var entered = new CountDownLatch(1);
var release = new CountDownLatch(1);
var first = executor.submit(a, () -> { entered.countDown(); release.await(); return 1; });
assertTrue(entered.await(2, TimeUnit.SECONDS));
assertEquals(2, executor.submit(b, () -> 2).get(2, TimeUnit.SECONDS));
release.countDown();
assertEquals(1, first.get(2, TimeUnit.SECONDS));
```
- [ ] Chạy `mvn -Dtest=PartitionExecutorTest test` đỏ.
- [ ] Bounded ready-partition queue tối đa partitionLimit; mỗi key chỉ một token pending/running. Runner xử lý một control hoặc data task rồi requeue nếu còn việc. Control dùng coalesced flag per partition, không một Runnable queue vô hạn; gộp các loại flush/completion qua state của runtime. `drain()` chỉ hoàn thành khi không còn queued/running task tại barrier. Không dùng CallerRunsPolicy làm disk chạy trên event loop.
```java
// Under the lane monitor, choose one task; execute outside that monitor.
Runnable task = lane.controlPending ? lane.takeControl() : lane.pollData();
task.run();
// Requeue this lane once if work remains; otherwise clear its scheduled flag.
```
`Lane` là nested class của PartitionExecutor, chứa controlPending, takeControl(), pollData(); exception của task hoàn thành future và không làm mất token/lane.
- [ ] Test reject/exception/cancel/drain races, same-partition FIFO, fairness khi một lane liên tục có việc; targeted suite xanh.
- [ ] Commit `feat: serialize partition work with bounded fair queues`.

## Task 6: Produce và flush coordinator

**Files:** tạo M/broker/{PartitionRuntime,FlushCoordinator}.java; T/broker/{ProduceDurabilityTest,FlushCoordinatorTest}.java; tạo T/support/BrokerHarness.java phần create/produce.

**Interfaces:** `PartitionRuntime(TopicPartition,PartitionStore,PartitionExecutor,DeadlineScheduler,BrokerConfig)`; `CompletableFuture<ProduceResult> produce(Batch,AckMode,long deadlineNanos)`; `void requestFlush()`; `long generation()`; `DeadlineScheduler.Ticket onChange(Runnable)`; `close()`. FlushCoordinator quản lý registration runtimes, `register(PartitionRuntime)`, `close()`; bookkeeping mutation chỉ trên lane của partition.

- [ ] Dùng Harness và clock: APPENDED xong không force, FLUSHED chưa xong trước 10 ms, 2 batch cùng được một flush bao phủ:
```java
var tp = harness.create("orders", 1);
var batch = new Batch(List.of(new LogRecord(0, null, new byte[]{1}, List.of())));
var pending = harness.request(new Produce(AckMode.FLUSHED, 1000,
    List.of(new ProduceEntry(tp, batch))));
harness.drain();
assertFalse(pending.isDone());
clock.advance(Duration.ofMillis(10)); harness.drain();
assertTrue(pending.isDone());
assertEquals(1, harness.store(tp).flushCalls());
```
- [ ] Chạy `mvn -Dtest=ProduceDurabilityTest,FlushCoordinatorTest test` đỏ.
- [ ] Pre-reserve waiter slot khi FLUSHED; append trên lane sau deadline check; dirty ledger entry `(nextOffset,storageEncodedBytes,appendNanoTime)`; cắt prefix <= durable marker ngay sau append/force. Dùng ledger head xác định oldest time, checked sum dirty bytes. Kernel:
```java
AppendResult appended = store.append(batch.records());
dirty.addLast(new Dirty(appended.nextOffset(), storageBytes, clock.nanoTime()));
long durable = store.durableEndOffset();
while (!dirty.isEmpty() && dirty.peekFirst().nextOffset() <= durable) {
    dirtyBytes -= dirty.removeFirst().bytes();
}
```
Nested Dirty record có ba trường trên; cộng dirtyBytes khi thêm; complete waiters theo durable. Metadata flush không dùng coordinator này. Coalesce flush trigger time/bytes; force lỗi markFailed + wake all; waiter timeout gỡ waiter nhưng dirty data vẫn flush nền. Record thực thi khi requester disconnect vẫn giữ charge đến khi mutation kết thúc.
- [ ] Test byte trigger (1 MiB cap thường cần nhiều batch), time trigger, hot appends không dời timer, real-storage rollover forces prefix, force failure, queued timeout trước append vs sau append, release waiter quota mọi nhánh. Targeted suite xanh.
- [ ] Commit `feat: support appended and flushed produce acknowledgments`.

## Task 7: Fetch budgets trên wire bytes

**Files:** tạo M/broker/FetchPlanner.java; sửa PartitionRuntime thêm read; T/broker/FetchPlannerTest.java.

**Interfaces:** runtime `CompletableFuture<FetchResult> read(FetchEntry,int remainingWireBudget,boolean allowFirstOversize)`; planner `CompletableFuture<FetchReply> read(Fetch)` nhận registry runtimes qua constructor. Duyệt entries theo thứ tự request, lần lượt await bằng composition, không block worker. Có thể không atomic giữa partitions.

- [ ] Test với hai partition, mỗi một batch 42 wire bytes (record tối thiểu 20 + 22), budget=1 chỉ trả một batch:
```java
var reply = (FetchReply) harness.request(new Fetch(1, 0, 0,
    List.of(new FetchEntry(a, 0, 1), new FetchEntry(b, 0, 1)))).get();
assertEquals(1, reply.results().stream().mapToInt(r -> r.batches().size()).sum());
assertEquals(1, reply.results().getFirst().batches().size());
```
Mở rộng Harness hỗ trợ immediate Fetch trong task này; a/b được create và append record tối thiểu trước snippet.
- [ ] Chạy `mvn -Dtest=FetchPlannerTest test` đỏ.
- [ ] Storage budget có thể dùng wireBudget + 8*maxBatchCount với checked cap, nhưng đơn giản/ít allocation hơn là đọc theo một batch/lần bằng `store.read(offset,1)` rồi encode-size check, tiến tới batch.nextOffset. Dừng khi budget hết; validate offset/status của entry ngay cả khi không còn budget. Reserve transient read space cho một max storage batch; không giữ kết quả vượt response reservation.
```java
int bytes = Math.addExact(22, RecordPayloadCodec.encodedSize(batch.records()));
boolean fits = bytes <= remainingTotal && bytes <= remainingPartition;
if (!fits && anyBatchReturned) break;
// If first batch does not fit, include it and stop data assembly globally.
```
- [ ] Test mid-batch offset (broker trả nguyên, client lọc sau), offset=end/outside, storage/wire difference, partial errors, no snapshot assumption, first oversize only, entry hết budget vẫn đúng bounds/error. Targeted suite xanh.
- [ ] Commit `feat: fetch complete batches within shared response budgets`.

## Task 8: Long polling và request aggregation

**Files:** tạo M/broker/{FetchCoordinator,RequestContext,RequestDispatcher}.java; T/broker/{LongPollTest,RequestDispatcherTest}.java; hoàn thiện Harness.

**Interfaces:** `RequestContext(long connectionId,long requestId,long deadlineNanos)` với cancellation registration và exactly-once completion; `RequestDispatcher.handle(RequestContext,Request): CompletableFuture<Response>`, `disconnect(long)`, `beginShutdown()`. FetchCoordinator `fetch(RequestContext,Fetch): CompletableFuture<FetchReply>`, `close()`.

- [ ] Đăng ký Fetch rồi append tại race giữa read và subscribe (latch/hook test), xác minh không đợi hết maxWaitMs:
```java
var waiting = harness.request(new Fetch(1024, 1, 500,
    List.of(new FetchEntry(tp, 0, 1024))));
harness.drain(); assertFalse(waiting.isDone());
harness.request(new Produce(AckMode.APPENDED, 1000,
    List.of(new ProduceEntry(tp, batch)))).get();
harness.drain(); assertTrue(waiting.isDone());
```
- [ ] Chạy `mvn -Dtest=LongPollTest,RequestDispatcherTest test` đỏ.
- [ ] Subscribe rồi đọc lại generation; một in-progress evaluation/dirty flag mỗi context. Nếu encoded returned bytes >= minBytes, có partition error, deadline maxWait hết hoặc shutdown thì complete snapshot; nếu chưa đủ release response data, giữ descriptors/subscriptions. CAS completion gỡ timer/subscriptions và lease đúng một lần. Dispatch Produce reserve per-entry queue/waiter trước mutation, collect theo thứ tự request, retain successes khi deadline đến; không all-or-nothing.
```java
if (completed.compareAndSet(false, true)) {
    timer.cancel();
    subscriptions.forEach(DeadlineScheduler.Ticket::cancel);
    result.complete(reply);
}
```
- [ ] Test minBytes=0, deadline empty/partial, errors wake, disconnect cleanup, repeated notifications coalesce, queue overload partial result, metadata errors, timeout giữ success trước đó. Targeted suite xanh.
- [ ] Commit `feat: coordinate bounded long polls and partial responses`.

## Task 9: Netty server transport với admission trước allocation

**Files:** sửa pom.xml; tạo M/transport/{ServerTransport,ClientTransport}.java và M/transport/netty/{BoundedFrameDecoder,NettyServerTransport}.java; T/transport/netty/{BoundedFrameDecoderTest,NettyServerTransportTest}.java.

**Interfaces:** ServerTransport `InetSocketAddress start(InetSocketAddress,RequestDispatcher) throws IOException`, `stopAccepting()`, `CompletableFuture<Void> closeAsync()`; ClientTransport `CompletableFuture<Void> connect(InetSocketAddress,Consumer<ResponseFrame>,Consumer<Throwable>)`, `CompletableFuture<Void> send(RequestFrame)`, `close()`. Transport reports connection events and request context IDs without exposing Netty core types.

- [ ] Add BOM `io.netty:netty-bom:4.2.18.Final` dependencyManagement; runtime modules netty-transport, netty-codec-base (4.2 split), netty-handler; check `mvn dependency:tree` before tests. EmbeddedChannel test partial frame one byte at a time, combined frames, overcap length rejected before reservation/allocation.
```java
var channel = new EmbeddedChannel(new BoundedFrameDecoder(limits, budget));
assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{0, 0})));
assertEquals(0, budget.used());
channel.finishAndReleaseAll();
```
Constructor receives ProtocolLimits and ResourceBudget from Tasks 2–3; output is owned frame bytes plus lease, released by dispatch completion.
- [ ] Chạy `mvn -Dtest=BoundedFrameDecoderTest,NettyServerTransportTest test` đỏ sau dependency resolution.
- [ ] Use incremental 4-byte prefix staging, validate then reserve full frame before payload allocation; do not let ByteToMessageDecoder unbounded cumulation defeat reservation. Decoder extends ChannelInboundHandlerAdapter, copies bounded chunks into reserved byte[], releases each inbound ByteBuf in finally. Before building record/header objects, preflight structure/counts and reserve an additional accounting charge of `2 * payloadBytes + 64 * recordCount + 64 * headerCount` with checked arithmetic. This is conservative admission accounting, not an exact JVM heap measurement. Release raw and decoded charges at their respective ownership endpoints. Per-connection sequential bounded validation stage preserves Produce order; short envelope work on event loop, full CRC/decode on 2-worker pool. Unknown op/version response must echo IDs. NIO loop creation:
```java
var group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
```
Own outbound reservation until ChannelFuture completion; slow peer over cap closes connection; release input lease only when queued/running work no longer references payload. Netty autoRead/backpressure must not block outbound flush/control. Reject duplicate active requestId by close to avoid ambiguous correlation. Partial-frame connection uses 30-second read-completion deadline then closes/releases lease.
- [ ] Test real TCP ephemeral port, fragmented/coalesced request, response inversion, validation delay preserving admission order, slow reader, connection cap, disconnect leases, leak detection paranoid. Targeted suite xanh.
- [ ] Commit `feat: serve bounded broker protocol over Netty TCP`.

## Task 10: Client connection và unknown outcomes

**Files:** tạo M/transport/netty/NettyClientTransport.java; M/client/{BrokerClient,ClientException}.java; T/support/LoopbackTransport.java; T/client/{BrokerClientTest,ClientOutcomeTest}.java.

**Interfaces:** `BrokerClient(ClientConfig,ClientTransport,DeadlineScheduler)`; `CompletableFuture<Response> request(Request)`; `close()`. ClientException có `Outcome { NOT_SENT, UNKNOWN }`, ErrorCode nếu có. LoopbackTransport implements ClientTransport, lưu sent frames, có `List<RequestFrame> sent()`, `reply(ResponseFrame)`, `failConnection(Throwable)`.

- [ ] Test hai requests response ngược thứ tự, timeout response muộn không khớp request mới; không gửi lại Produce:
```java
var first = client.request(produce);
transport.failConnection(new IOException("lost response"));
assertTrue(first.isCompletedExceptionally());
assertEquals(1, transport.sent().size());
```
Connect harness trước send; advance ManualScheduler/drain callback deterministic, không dựa timing mạng.
- [ ] Chạy `mvn -Dtest=BrokerClientTest,ClientOutcomeTest test` đỏ.
- [ ] Pending table key `(connectionGeneration,requestId)`, monotonic id đến MAX rồi reconnect sau fail/drain old requests. Mark MAY_HAVE_SENT trước gọi transport.send; send future success chỉ write completion, không phải broker ACK. Local admission/deadline-before-send = NOT_SENT; mọi timeout/disconnect sau attempt = UNKNOWN. Decoder mismatched response op/version đóng connection. Broker known per-entry errors giữ nguyên, không collapse partial success thành request-wide retry.
```java
pending.markMayHaveSent();
transport.send(frame).whenComplete((ignored, error) -> {
    if (error != null) pending.fail(ClientException.unknown(error));
});
```
Pending là nested state trong BrokerClient; static factory ClientException.unknown(Throwable) được định nghĩa ở task này. New request có thể initiate reconnect, không replay old pending.
- [ ] Test in-flight=32, local buffer rejection, callback cancellation, request ID exhaustion, reconnection generation, no silent retry, Netty client loopback against server. Targeted suite xanh.
- [ ] Commit `feat: correlate client requests without ambiguous produce retries`.

## Task 11: Producer batching/routing và explicit consumer

**Files:** tạo M/client/{Partitioner,BatchAccumulator,Producer,Consumer}.java; T/client/{PartitionerTest,ProducerTest,ConsumerTest}.java.

**Interfaces:** `Partitioner.forKey(byte[],int): int`; `Producer(BrokerClient,ClientConfig,DeadlineScheduler)`; nested `RecordMetadata(TopicPartition,long offset)`; `send(String topic,Integer partition,LogRecord,AckMode): CompletableFuture<RecordMetadata>`; `close()`. BatchAccumulator groups `(topicId,partition,ack)` because one request uses one ack; never mix modes in a batch. Consumer `Consumer(BrokerClient)`, `fetch(List<FetchEntry>,int maxBytes,int minBytes,int maxWaitMs): CompletableFuture<List<PartitionRecords>>`; nested `FetchedRecord(long offset,LogRecord record)` and `PartitionRecords(TopicPartition,Error,long start,long end,List<FetchedRecord>)`.

- [ ] Hash golden: CRC32C("123456789") = 0xe3069283, modulo 7 = use unsigned literal computation below; null routing tests via Producer, empty key hashed:
```java
assertEquals((int)(0xe3069283L % 7),
    Partitioner.forKey("123456789".getBytes(StandardCharsets.US_ASCII), 7));
assertEquals(0, Partitioner.forKey(new byte[0], 7));
```
- [ ] Chạy `mvn -Dtest=PartitionerTest,ProducerTest,ConsumerTest test` đỏ.
- [ ] Explicit partition validate metadata; non-null key CRC; null records stay on current round-robin lane until that null-key batch seals by size/count/linger, then advance counter. Preserve call order within selected partition even when ack differs: seal prior batch before new mode. Max one batch/partition/request; keep later batches in send FIFO. Metadata lookup happens under send deadline and buffer reservation, callback stale/cancel must not enqueue expired records. Frame-size preflight before grouping.
```java
CRC32C crc = new CRC32C();
crc.update(key, 0, key.length);
return (int)(crc.getValue() % partitionCount);
```
Consumer rotates request entries per call; maps results back by key; filters `baseOffset+i < requestedOffset`, không tự commit/reset. Per-record deadline nếu hết trước batch gửi thì remove/NOT_SENT; after send UNKNOWN. Close seals/drains accepted batches within deadline without upgrading APPENDED durability.
- [ ] Test 64 KiB/5 ms/10.000 limits, record lớn riêng, oversized record không phá batch trước, mixed mode order, buffer=16 MiB, unknown metadata, lost response giữ UNKNOWN, consumer mid-batch and rotation, close timeout. Targeted suite xanh.
- [ ] Commit `feat: batch producer records and fetch explicit consumer offsets`.

## Task 12: Broker composition và lifecycle

**Files:** tạo M/broker/Broker.java; T/broker/{BrokerLifecycleTest,BrokerIsolationTest}.java.

**Interfaces:** `Broker.start(BrokerConfig): Broker` throws IOException; `InetSocketAddress address()`; `CompletableFuture<Void> shutdown(Duration)`; `close()`. Nội bộ overload nhận PartitionStore.Factory và DeadlineScheduler cho faults; production dùng file store/system clock.

- [ ] Test lock root và restart, listener chưa mở khi metadata recovery chưa xong:
```java
try (var broker = Broker.start(BrokerConfig.defaults(dir).withPort(0))) {
    assertTrue(broker.address().getPort() > 0);
    assertThrows(IOException.class,
        () -> Broker.start(BrokerConfig.defaults(dir).withPort(0)));
}
```
- [ ] Chạy `mvn -Dtest=BrokerLifecycleTest,BrokerIsolationTest test` đỏ.
- [ ] Root `.broker.lock` trước metadata; cleanup reverse order trên startup failure. Wire metadata fatalHandler to one-shot shutdown. Shutdown stop accepts/admission, wake fetches, drain accepted jobs, force partitions, complete responses, close resources. Deadline không cho đóng store giữa đang write: hết deadline đóng clients và hoàn thành shutdown exceptionally; root lock chỉ nhả khi I/O/store thực sự đóng, không giả vờ đã safe. CLI process có thể bị terminate sau deadline; library không System.exit.
```java
if (state.compareAndSet(State.RUNNING, State.STOPPING)) {
    transport.stopAccepting();
    dispatcher.beginShutdown();
}
```
State là enum nested của Broker; stop sequence chạy lifecycle worker, không event loop. Close lỗi phải collect/suppress, vẫn cố đóng tài nguyên khác.
- [ ] Test metadata corruption fail startup; one data failure still healthy traffic; shutdown FLUSHED waiter, long poll, queued work; stalled I/O deadline, repeated shutdown, cleanup locks/threads. `mvn test` toàn bộ xanh.
- [ ] Commit `feat: manage broker startup recovery and bounded shutdown`.

## Task 13: Fault và resource conformance qua process/network thật

**Files:** tạo T/integration/{BrokerProcess,BrokerCrashTest,BrokerNetworkFaultTest,BrokerBackpressureTest}.java; T/transport/TransportConformanceTest.java.

**Interfaces:** BrokerProcess test helper launches Java subprocess with data dir/port, writes READY port line after start; stdin STOP requests shutdown, parent may destroyForcibly. Network fault tests dùng Java ServerSocket relay có latch, không phụ thuộc proxy service bên ngoài. Conformance suite nhận Supplier<ClientTransport> và server factory để Java NIO tương lai tái sử dụng.

- [ ] Viết subprocess FLUSHED test và socket relay drop-response test; producer record key riêng để đếm sau reconnect:
```java
Process child = new ProcessBuilder(javaBinary, "-cp", System.getProperty("java.class.path"),
    "vn.huyqt.logbroker.integration.BrokerProcess", dir.toString(), "0")
    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
// Read READY with a bounded future, send FLUSHED via BrokerClient, await its result.
child.destroyForcibly();
assertTrue(child.waitFor(10, TimeUnit.SECONDS));
```
javaBinary = Path.of(System.getProperty("java.home"),"bin","java").toString(); stream reader must have bounded line length and timeout. Record input/result used by test retained in parent, not inferred from child's stdout alone.
- [ ] Chạy `mvn -Dtest=BrokerCrashTest,BrokerNetworkFaultTest,BrokerBackpressureTest,TransportConformanceTest test`; tạo fault hooks trước để test thực sự đỏ vì thiếu cleanup/behavior nếu phát hiện, không sửa assertions để hợp thức hóa lỗi.
- [ ] Implement missing cleanup/fixes tại file sở hữu hành vi, không thêm network retry. Relay accepts two sockets, forwards bounded chunks, drops only response after observed completed Produce; then Fetch verifies at most one auto-attempt. Resource tests inject capacities nhỏ, saturate từng budget, unblock rồi verify count về baseline:
```java
assertEquals(0, inputBudget.used());
assertEquals(0, outboundBudget.used());
assertEquals(0, waiterBudget.used());
```
Budgets lấy qua package-private diagnostic snapshot của BrokerHarness/transport test constructor, không public metrics framework. Exercise decoded amplification bằng nhiều tiny headers/counts; bound retained structures bằng encoded bytes + count limits, không claim exact heap byte cap.
- [ ] Chạy integration suite và `mvn clean verify`. Required: fragmentation/coalescing, out-of-order responses, bounded slow peers, disconnect cleanup, crash recovery, partial partition failure, no retries; lưu test report paths trong execution report.
- [ ] Commit `test: verify broker crash and network failure contracts`.

## Task 14: Runnable demo, cấu hình và handoff

**Files:** tạo M/broker/BrokerMain.java, M/example/ClientExample.java; docs/broker-configuration.md; T/integration/BrokerExampleTest.java; sửa pom.xml để pin dependency plugin, README.md và docs/protocol-v1.md nếu cần đối chiếu implementation.

**Interfaces:** BrokerMain args `--data <path> --host <host> --port <int>`, defaults host=127.0.0.1/port=9092, data bắt buộc; optional `--config <properties-file>` cho mọi limit spec §11 bằng tên camelCase. ClientExample args `<host> <port> <topic>`; create 2 partitions, produce FLUSHED, fetch đúng offsets, print deterministic success. Dùng System.Logger, không thêm logging dependency.

- [ ] ExampleTest khởi động ephemeral broker và chạy ClientExample, assert record count/content, repeat same topic preserves ID:
```java
try (var broker = Broker.start(BrokerConfig.defaults(dir).withPort(0))) {
    ClientExample.main(new String[]{"127.0.0.1",
        Integer.toString(broker.address().getPort()), "demo"});
}
```
- [ ] Chạy `mvn -Dtest=BrokerExampleTest test` đỏ.
- [ ] Implement CLI strict parsing, reject unknown keys; `--config` defaults then file then CLI overrides. Broker shutdown hook calls shutdown deadline; ready line chỉ sau listener ready. ClientExample dùng try/finally close clients. README lệnh Windows:
```powershell
mvn clean verify dependency:copy-dependencies
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.broker.BrokerMain --data target/broker-data --port 9092
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.example.ClientExample 127.0.0.1 9092 demo
```
Pin maven-dependency-plugin 3.8.1 trong pom.xml nếu dùng goal này trong docs; đây là build-only dependency. Config doc liệt kê mọi property/default/unit/range/cross-validation, ACK vs flush vs visibility, UNKNOWN retry duplicates, budget exception, trusted lab scope, strict recovery, shutdown và limits.
- [ ] Chạy `mvn clean verify`, CLI smoke ở process riêng, restart cùng data và Fetch lại; `git diff --check`. Kiểm tra code không import Netty ngoài transport.netty và không thêm unbounded queues; không gọi test pass nếu lệnh bị chặn/download lỗi.
- [ ] Commit `docs: add runnable broker and client learning examples`.

## Checkpoints và coverage review

| Spec | Task và bằng chứng |
| --- | --- |
| 1–3 scope/boundaries | 1–3, 9, 14; payload golden + import boundary |
| 4 metadata | 4,12,13; durable create/replay, ID/idempotence, corruption |
| 5 protocol | 2,9,10; literal fixtures và framing faults |
| 6 acknowledgments/flush | 5,6,8; time/bytes, partial rollover, FAILED, waiter cleanup |
| 7 Fetch | 7,8,11; wire budget, first batch exception, lost wakeup, offset filtering |
| 8 execution/backpressure | 3,5,8–10,13; every cap hit/released, FIFO và fair control |
| 9 client | 10,11; no retry, UNKNOWN, CRC routing, batching, deadline/cancel/close |
| 10 lifecycle | 4,12,13; root lock, isolation, graceful/deadline shutdown |
| 11 defaults | 3,9,11,14; literal config tests + config reference |
| 12 acceptance | 13,14; full suite + subprocess/CLI evidence |
| 13 later phases | 2,4,6,14; local metadata source separated, no consumer state in log |

Review checkpoints: sau Task 3 (format/limits), sau Task 8 (broker core), sau Task 11 (end-to-end client), sau Task 14 (faults/docs). Không mở rộng sang optimization/replication khi tests hiện tại qua. Nếu thực thi phát hiện spec mâu thuẫn thực tế, ghi rõ thay đổi hợp đồng để review, không tự thay ACK/recovery semantics.

## Nguồn dependency/API đã kiểm tra khi lập plan

- [Netty 4.2.18.Final release](https://netty.io/news/2026/09/09/4-2-18-Final.html).
- [MultiThreadIoEventLoopGroup](https://netty.io/4.2/api/io/netty/channel/MultiThreadIoEventLoopGroup.html) và [NioIoHandler](https://netty.io/4.2/api/io/netty/channel/nio/NioIoHandler.html) cho NIO transport.
- [Maven dependency:copy-dependencies 3.8.1](https://maven.apache.org/plugins-archives/maven-dependency-plugin-3.8.1/copy-dependencies-mojo.html) cho runtime classpath của demo.
- Versions Maven compiler 3.13.0, Surefire 3.5.2 và JUnit 5.11.4 được giữ từ pom.xml hiện có. Dependency resolution/build chưa chạy trong phiên chỉ viết tài liệu này.

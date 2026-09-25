# Persistent Partition Log — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây thư viện Java lưu log một partition theo batch, đọc theo offset, flush tường minh và recovery nghiêm ngặt.

**Architecture:** PartitionLog điều phối LogSegment, BatchCodec, OffsetIndex và LogRecovery. Data log là nguồn dữ liệu chuẩn; index luôn có thể rebuild. Serialize mutation bằng write lock, dùng positional read và không đưa trạng thái consumer vào storage.

**Tech Stack:** Java 21, Maven 3.9.x, JUnit Jupiter 5.11.4 cho test; không dependency runtime. Dùng FileChannel, ByteBuffer, CRC32C, ReentrantReadWriteLock và System.Logger của JDK.

**Spec:** [Thiết kế Phase 1 đã duyệt](../specs/2026-09-24-storage-phase-1-design.md). Đọc cả spec lẫn plan trước khi thực hiện.

## Global Constraints

- Tách append và flush; append thành công chưa bảo đảm dữ liệu tồn tại sau mất điện.
- Lưu theo record batch ngay từ Phase 1.
- Recovery nghiêm ngặt: chỉ sửa phần đuôi ghi thiếu trong active segment; checksum sai trên batch đầy đủ hoặc segment cũ hỏng phải báo lỗi.
- Storage không chứa trạng thái ACK, group, owner hoặc acquisition lock; hỗ trợ Share Groups về sau bằng lớp riêng.
- Chưa bao gồm compression, retention tự động, compaction, transaction, async I/O, memory mapping hoặc benchmark tối ưu.
- Không cam kết tương thích binary format của Kafka.
- Các số nhiều byte dùng big-endian; offset và timestamp dùng signed 64-bit.
- Cấu hình mặc định đề xuất: segment 64 MiB, max batch 1 MiB, sparse index interval 4 KiB.
- Phase 1 không cam kết truncate nhiều file là atomic khi crash; caller phải retry về offset đích sau recovery.
- Không tuyên bố kiểm thử process kill chứng minh an toàn trước mất điện.

## Bối cảnh và lựa chọn triển khai

Repository hiện chỉ có README, .gitignore và spec. Root thực thi mọi lệnh là `D:/User2/distributed-system-labs/java-log-broker`, không phải thư mục cha hay repository `../kafka`.

Đã kiểm tra môi trường: JDK 21.0.1 và Maven 3.9.12. Java 21 là lựa chọn cho plan, không phải version floor có sẵn trong spec. Dùng một Maven module đóng gói JAR; chỉ tách module khi phase sau cần. Không thêm Spring, Netty hoặc Kafka dependency vào Phase 1.

Thực hiện task theo thứ tự. Mỗi bước code làm theo test đỏ → implementation nhỏ nhất → test xanh; các case trong bảng acceptance là bắt buộc, không chỉ chạy ví dụ minh họa. Chạy test mục tiêu sau mỗi thay đổi; toàn suite ở checkpoint tích hợp. Lỗi tải dependency không phải bằng chứng test đỏ; phải giải quyết môi trường trước. Không thực hiện implementation trong phiên chỉ viết plan.

## Bản đồ file

Tất cả file Java production dưới `src/main/java/vn/huyqt/logbroker/storage/` (ký hiệu **M** bên dưới). Test dưới `src/test/java/vn/huyqt/logbroker/storage/` (**T**); mọi file cùng package `vn.huyqt.logbroker.storage`, trừ example. Ký hiệu chỉ rút gọn đường dẫn, không phải thư mục mới.

| File | Trách nhiệm |
| --- | --- |
| `pom.xml` | Build/test có version cố định |
| M/LogConfig.java | Giới hạn segment, batch, index |
| M/LogRecord.java, M/RecordHeader.java | Giá trị bất biến, defensive copy byte arrays |
| M/RecordBatch.java, M/AppendResult.java | Kết quả đọc và khoảng offset đã append |
| M/CorruptLogException.java | Lỗi format/data corruption, extends IOException |
| M/BatchCodec.java | Binary layout v1, CRC, bounded decoding |
| M/LogIo.java | Điểm inject hẹp cho write, force, truncate, delete |
| M/LogSegment.java | Data file và positional I/O, không sở hữu group state |
| M/OffsetIndex.java | Sparse index, floor lookup và rebuild |
| M/LogRecovery.java | Scan không sửa data, sau đó repair/rebuild có kiểm soát |
| M/PartitionLog.java | Lifecycle, lock, append/read/flush/truncate và trạng thái lỗi |
| M/example/StorageExample.java | Ví dụ chạy hoàn chỉnh |
| T/StorageFixtures.java | Fixture record/batch, ghi file test, không mocking codec |
| T/ScriptedLogIo.java | Inject short write, fail, pause; chỉ dùng trong test |
| T/CrashWriter.java | Tiến trình con dùng cho crash/lock tests |
| `docs/storage-format-v1.md` | Byte layout, hợp đồng và giới hạn |

Test files cụ thể được chỉ định trong từng task. Không tạo trước tất cả class dưới dạng stub; mỗi task tạo phần nó cần.

## Contract dùng chung

Public types được xây ở task 1, codec ở task 2, PartitionLog ở task 6–8. Constructor/accessor/model/config defaults và API PartitionLog dưới đây đều public, ngoại trừ overload open có LogIo; codec/segment/index/recovery/I/O giữ package-private:

```java
// Class bất biến; accessor trả copy đối với byte[].
new RecordHeader(String key, byte[] value);
new LogRecord(long timestamp, byte[] key, byte[] value, List<RecordHeader> headers);
new RecordBatch(long baseOffset, List<LogRecord> records, int encodedSize);
// Accessors cùng tên field; RecordBatch.nextOffset() trả exclusive end.
record AppendResult(long firstOffset, long nextOffset) {}
record LogConfig(long segmentBytes, int maxBatchBytes, int indexIntervalBytes) {
    static LogConfig defaults();
}

// Toàn bộ mutation/read có IOException khi thao tác filesystem.
static PartitionLog open(Path directory, LogConfig config) throws IOException;
// Overload package-private cho fault tests; default open dùng new LogIo().
static PartitionLog open(Path directory, LogConfig config, LogIo io) throws IOException;
AppendResult append(List<LogRecord> records) throws IOException;
List<RecordBatch> read(long offset, int maxBytes) throws IOException;
long flush() throws IOException;
void truncateTo(long offset) throws IOException;
long logStartOffset();
long logEndOffset();
long durableEndOffset();
void close() throws IOException;
```

Constructor của model/config reject null bắt buộc bằng NullPointerException và giá trị không hợp lệ bằng IllegalArgumentException. Invalid caller offset/budget/batch: IllegalArgumentException. Dữ liệu trên đĩa sai: CorruptLogException chứa path, byte position và lý do khi có thông tin file. FAILED/CLOSED: IllegalStateException khi thao tác tiếp, giữ cause I/O đầu tiên cho FAILED. Offset getters dùng read lock và yêu cầu OPEN; close idempotent. Không public FileChannel/ByteBuffer.

## Task 1: Build và model bất biến

**Files:** Create `pom.xml`; M/LogConfig.java, M/LogRecord.java, M/RecordHeader.java, M/RecordBatch.java, M/AppendResult.java; T/LogModelTest.java, T/StorageFixtures.java.

**Interfaces:** Consumes JDK. Produces các model trong contract; `StorageFixtures.record(String)` trả record timestamp 0, key null, UTF-8 value, headers rỗng; `StorageFixtures.records(String...)` trả list tương ứng.

- [ ] Tạo cấu hình build tối thiểu để chạy test model (không cần task scaffold độc lập):

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>vn.huyqt.logbroker</groupId><artifactId>java-log-broker</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId>
      <version>5.11.4</version><scope>test</scope>
    </dependency>
  </dependencies>
  <build><plugins>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>3.5.2</version></plugin>
  </plugins></build>
</project>
```

- [ ] Viết test đỏ, có imports JUnit `Test`, assertions và java.util/java.nio khi cần:

```java
@Test void protectsRecordBytesAndPreservesNull() {
    byte[] bytes = {1, 2};
    var record = new LogRecord(0, null, bytes, List.of(new RecordHeader("x", null)));
    bytes[0] = 9;
    record.value()[1] = 9;
    assertArrayEquals(new byte[]{1, 2}, record.value());
    assertNull(record.key());
    assertNull(record.headers().getFirst().value());
}
```

- [ ] Chạy `mvn "-Dtest=LogModelTest" test`; expected compilation failure do model chưa có.
- [ ] Implement models với copy tại constructor/accessor, `List.copyOf`, content-based equals/hashCode cho byte arrays. Dùng `Math.addExact(baseOffset, records.size())` kiểm tra nextOffset; reject baseOffset âm, batch rỗng, encodedSize < 30. Config yêu cầu segmentBytes >= maxBatchBytes >= 50, interval > 0. Record tối thiểu 20 bytes; batch tối thiểu 50 bytes.

```java
private static byte[] copy(byte[] value) {
    return value == null ? null : value.clone();
}
// Trong RecordBatch constructor:
if (baseOffset < 0 || records.isEmpty() || encodedSize < 30) {
    throw new IllegalArgumentException("Invalid batch");
}
Math.addExact(baseOffset, records.size());
```

- [ ] Thêm case headers trùng key có thứ tự, null vs empty, mutation list nguồn, offset overflow, config invalid; chạy `mvn "-Dtest=LogModelTest" test`, expected PASS.
- [ ] Commit file task: `git add pom.xml src` rồi `git commit -m "feat: add storage models and Java build"`.

## Task 2: Batch codec v1 và chống dữ liệu không hợp lệ

**Files:** Create M/BatchCodec.java, M/CorruptLogException.java; T/BatchCodecTest.java. Modify T/StorageFixtures.java.

**Interfaces:** Consumes models task 1. Produces `static byte[] BatchCodec.encode(long baseOffset, List<LogRecord> records, int maxBatchBytes)`; `static RecordBatch decode(byte[] bytes, int maxBatchBytes) throws CorruptLogException`; `static void validateHeaderPrefix(byte[] prefix, int maxBatchBytes) throws CorruptLogException`. Prefix dài tối đa 30, kiểm tra tất cả trường/byte magic/version/attributes đã có đủ thông tin. CorruptLogException có constructor `(String message)` và `(String message, Throwable cause)`.

- [ ] Viết test round-trip và CRC corruption:

```java
@Test void roundTripAndRejectCorruption() throws Exception {
    var records = StorageFixtures.records("a", "b");
    byte[] bytes = BatchCodec.encode(7, records, 1024);
    var decoded = BatchCodec.decode(bytes, 1024);
    assertEquals(7, decoded.baseOffset());
    assertEquals(9, decoded.nextOffset());
    assertEquals(records, decoded.records());
    bytes[bytes.length - 1] ^= 1;
    assertThrows(CorruptLogException.class, () -> BatchCodec.decode(bytes, 1024));
}
```

- [ ] Chạy `mvn "-Dtest=BatchCodecTest" test`; expected FAIL vì codec chưa tồn tại.
- [ ] Implement two-pass sizing/encoding: tính size bằng long, checked arithmetic, so với max trước allocate; kiểm tra UTF-8 header key bằng encoder REPORT. Byte layout:

| Byte position | Field | Bytes |
| --- | --- | --- |
| 0 | ASCII DLOG | 4 |
| 4 | version = 1 | 2 |
| 6 | totalLength | 4 |
| 10 | baseOffset | 8 |
| 18 | recordCount | 4 |
| 22 | attributes = 0 | 4 |
| 26 | CRC32C | 4 |
| 30 | records | variable |

```java
CRC32C crc = new CRC32C();
crc.update(bytes, 0, 26);
crc.update(bytes, 30, bytes.length - 30);
ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(26, (int) crc.getValue());
```

- [ ] Implement bounded decoder: kiểm tra header và CRC trước nested payload; totalLength == bytes.length; count > 0 và count <= payloadBytes / 20; nullable length chỉ -1 hoặc >= 0; mọi length <= remaining; headerCount <= remaining / 8; UTF-8 key decode REPORT; tiêu thụ hết payload. Không allocate list theo count chưa kiểm chứng. Overflow từ disk đổi thành CorruptLogException; overflow caller encode thành IllegalArgumentException.
- [ ] Thêm parameterized tests cho version, attributes, negative/oversize lengths, count, offset overflow, malformed UTF-8, trailing bytes. Khi muốn test payload validation, tính lại CRC trong fixture bằng đoạn CRC trên để không dừng ở CRC lỗi.
- [ ] Thêm golden fixture xây độc lập: một record timestamp 0, key null, value `{65}`, headers rỗng, baseOffset 0, totalLength 51. Tạo header/payload literal và CRC độc lập trong test; so sánh từng byte với encode, decode fixture. Kiểm tra prefix thiếu từng byte và prefix magic sai ngay byte đầu.
- [ ] Chạy `mvn "-Dtest=LogModelTest,BatchCodecTest" test`; expected PASS; commit `feat: implement bounded batch codec with CRC32C` (stage đúng file task).

## Task 3: Segment I/O và short writes

**Files:** Create M/LogIo.java, M/LogSegment.java; T/LogSegmentTest.java, T/ScriptedLogIo.java.

**Interfaces:** Consumes BatchCodec. `LogIo` là class package-private, các method overridable: `int write(FileChannel channel, ByteBuffer src, long position)`, `void force(FileChannel channel)`, `void truncate(FileChannel channel, long size)`, `void delete(Path path)`; đều throws IOException. Default delegate JDK, force dùng `channel.force(true)`.

`LogSegment.open(Path path, long baseOffset, LogIo io)` mở READ/WRITE/CREATE; implements AutoCloseable. Methods: `long append(byte[] batch)`, `byte[] readBytes(long position, int length)`, `long size()`, `void force()`, `void truncate(long bytes)`, `void close()` throws IOException; `Path path()`, `long baseOffset()`. append trả file position trước write; không tự force. `static Path dataPath(Path directory, long baseOffset)` dùng `%020d.log` với Locale.ROOT.

- [ ] Viết test short write thực sự sử dụng delegate I/O:

```java
@Test void completesShortWrites(@TempDir Path dir) throws Exception {
    var io = new ScriptedLogIo();
    io.maxWriteBytes = 3;
    byte[] bytes = BatchCodec.encode(0, StorageFixtures.records("abc"), 1024);
    try (var segment = LogSegment.open(LogSegment.dataPath(dir, 0), 0, io)) {
        assertEquals(0, segment.append(bytes));
        assertArrayEquals(bytes, segment.readBytes(0, bytes.length));
        assertEquals(0, io.forceCalls);
    }
}
```

- [ ] Chạy `mvn "-Dtest=LogSegmentTest" test`; expected FAIL vì thiếu segment/I/O seam.
- [ ] Implement write/read loop dùng explicit position, close không force; không mở APPEND. Limit source view để inject short write, rồi advance source position theo actual bytes written.

```java
long position = channel.size();
long start = position;
ByteBuffer buffer = ByteBuffer.wrap(batch);
int zeroProgress = 0;
while (buffer.hasRemaining()) {
    int n = io.write(channel, buffer, position);
    if (n < 0 || (n == 0 && ++zeroProgress >= 16)) {
        throw new IOException("Write made no progress");
    }
    if (n > 0) zeroProgress = 0;
    position = Math.addExact(position, n);
}
return start;
```

- [ ] Implement ScriptedLogIo fields `maxWriteBytes` (default Integer.MAX_VALUE), `forceCalls`, `failForce` (boolean), `failAfterBytes` (long, default Long.MAX_VALUE), `writtenBytes`; forceCalls đếm attempt và failForce ném trước delegate. write chia nhỏ tại failAfterBytes, call sau ném IOException. Thêm zero-write subclass riêng cho test; seam chưa cần concurrency hooks ở task này.
- [ ] Test positional reads không ảnh hưởng nhau, EOF throws EOFException, force có gọi, truncate đúng size, zero progress hữu hạn và IOException sau partial write. Chạy `mvn "-Dtest=LogSegmentTest" test`; expected PASS.
- [ ] Commit `feat: add segment file IO and deterministic fault injection`.

## Task 4: Sparse offset index có thể tái tạo

**Files:** Create M/OffsetIndex.java; T/OffsetIndexTest.java.

**Interfaces:** `record OffsetIndex.Entry(long offset, long position)`; `new OffsetIndex(int intervalBytes)`; `void consider(long offset, long position)`; `Entry floor(long offset)` trả null nếu trước entry đầu; `List<Entry> entries()` immutable; `void write(Path path, LogIo io) throws IOException`; `static Path indexPath(Path directory, long baseOffset)`. File index là chuỗi cặp int64 offset/int64 position, big-endian, không cần format chuẩn Kafka. Nguồn index khi mở log luôn rebuild từ scan, không trust file cũ.

- [ ] Viết test lookup và khoảng cách thưa:

```java
@Test void indexesFirstBatchAndByteIntervals() {
    var index = new OffsetIndex(100);
    index.consider(0, 0);
    index.consider(2, 60);
    index.consider(4, 120);
    assertEquals(new OffsetIndex.Entry(0, 0), index.floor(3));
    assertEquals(new OffsetIndex.Entry(4, 120), index.floor(4));
    assertNull(index.floor(-1));
}
```

- [ ] Chạy `mvn "-Dtest=OffsetIndexTest" test`; expected FAIL vì class chưa có.
- [ ] Implement sorted entries và binary floor search. `consider` luôn thêm batch đầu; thêm nếu position - lastIndexedPosition >= interval. Validate offset/position tăng nghiêm ngặt đối với mọi lần consider, kể cả batch không được index.

```java
int lo = 0, hi = entries.size() - 1, found = -1;
while (lo <= hi) {
    int mid = lo + (hi - lo) / 2;
    if (entries.get(mid).offset() <= offset) { found = mid; lo = mid + 1; }
    else hi = mid - 1;
}
return found < 0 ? null : entries.get(found);
```

- [ ] `write` mở index CREATE/WRITE/TRUNCATE_EXISTING, ghi đầy đủ qua LogIo.write, đóng; không force index. Không để partial index làm data corruption. Task 5 sẽ overwrite index sau data scan.
- [ ] Test empty, exact boundary, giữa hai entries, cuối, monotonic validation, file bytes và viết đè index rác. Chạy `mvn "-Dtest=OffsetIndexTest" test`; expected PASS.
- [ ] Commit `feat: add rebuildable sparse offset index`.

## Task 5: Recovery scan trước, sửa sau

**Files:** Create M/LogRecovery.java; T/LogRecoveryTest.java. Modify T/StorageFixtures.java.

**Interfaces:** `static LogRecovery.Result recover(Path directory, LogConfig config, LogIo io) throws IOException`. Nested immutable records: `SegmentInfo(long baseOffset, long validBytes, long nextOffset, OffsetIndex index)` và `Result(List<SegmentInfo> segments, long nextOffset)`. Nếu không có data file, trả list rỗng/end 0; caller tạo active segment. Không tự acquire directory lock; PartitionLog giữ lock trước gọi. `StorageFixtures.writeBatch(Path dir, long segmentBase, byte[] bytes)` append bytes vào data file bằng Files.write(CREATE, APPEND).

- [ ] Viết test tách torn tail với corrupt batch đầy đủ:

```java
@Test void truncatesOnlyIncompleteActiveTail(@TempDir Path dir) throws Exception {
    byte[] a = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
    byte[] b = BatchCodec.encode(1, StorageFixtures.records("b"), 1024);
    StorageFixtures.writeBatch(dir, 0, a);
    StorageFixtures.writeBatch(dir, 0, Arrays.copyOf(b, b.length - 1));
    var result = LogRecovery.recover(dir, new LogConfig(4096, 1024, 64), new LogIo());
    assertEquals(1, result.nextOffset());
    assertEquals(a.length, Files.size(LogSegment.dataPath(dir, 0)));
}
@Test void rejectsCompleteBadChecksumWithoutMutation(@TempDir Path dir) throws Exception {
    byte[] bytes = BatchCodec.encode(0, StorageFixtures.records("a"), 1024);
    bytes[bytes.length - 1] ^= 1;
    StorageFixtures.writeBatch(dir, 0, bytes);
    assertThrows(CorruptLogException.class,
        () -> LogRecovery.recover(dir, new LogConfig(4096, 1024, 64), new LogIo()));
    assertArrayEquals(bytes, Files.readAllBytes(LogSegment.dataPath(dir, 0)));
}
```

- [ ] Chạy `mvn "-Dtest=LogRecoveryTest" test`; expected FAIL vì recovery chưa có.
- [ ] Implement scan: chỉ nhận data filename đúng 20 chữ số và .log, parse không overflow; tên .log không hợp lệ là lỗi. Bỏ qua .lock/.index; unknown file không bị xóa. Sort numeric, segment đầu phải base 0 ở Phase 1. Với mỗi batch: đọc tối đa header, validate prefix trước allocation, validate length/count/base/expected offset, decode đầy đủ nếu đủ bytes. Kiểm tra cả thông tin offset đã đủ trong partial header; mismatch không được phân loại torn tail.

```text
for each segment in increasing base offset:
  require segment.base == expectedNextOffset
  for each batch from byte position 0:
    read bounded header prefix; validate every available field
    if incomplete header/body:
      require this is the active segment
      record repair position; stop this segment scan
    else decode CRC + payload; require batch.base == expectedNextOffset
      index.consider(batch.base, position)
      advance expectedNextOffset and position using checked arithmetic
  reject empty sealed segment
after ALL scans succeed:
  truncate recorded incomplete active tail through LogIo
  rebuild every index; force every data file
  return scanned metadata (no open file handles)
```

- [ ] Không áp dụng repair/index overwrite nếu bất kỳ data corruption nào được tìm thấy. Sau repair log WARNING gồm path, original size, validBytes, bytesRemoved. IOException trong repair/rebuild/force làm open thất bại; luôn close mọi channel. Không giữ danh sách toàn bộ record khi scan, chỉ một batch và sparse entries.
- [ ] Thêm test parameterized cắt batch cuối ở mọi vị trí 1..length-1; prefix header đã sai phải reject. Test sealed tail, full CRC error, offset gap/duplicate, malformed names, first base != 0, empty sealed, empty active, unknown version, oversized length, malformed payload với CRC đúng. Snapshot bytes của mọi .log trước corruption test, assert nguyên vẹn sau failure.
- [ ] Test index thiếu/rác/truncated đều rebuild; force failure không trả Result thành công; cấu hình maxBatchBytes nhỏ hơn batch đang lưu bị reject rõ ràng, không truncate.
- [ ] Chạy `mvn "-Dtest=BatchCodecTest,LogRecoveryTest" test`; expected PASS; commit `feat: recover logs with strict corruption handling`.

## Task 6: Partition lifecycle, append/read và rollover

**Files:** Create M/PartitionLog.java; T/PartitionLogTest.java. Modify M/LogSegment.java nếu cần accessor đã định nghĩa; không thêm public file handle.

**Interfaces:** Consumes LogRecovery.Result, LogSegment, OffsetIndex, LogIo và codec. Produces PartitionLog contract chung ngoại trừ truncateTo (task 8); flush/close được implement thành công ở task này, fault semantics mở rộng task 7. Fields chính: TreeMap<Long, LogSegment>, indexes cùng key, logEndOffset, durableEndOffset, ReentrantReadWriteLock, state OPEN/FAILED/CLOSED, IOException firstFailure, lock channel/FileLock.

- [ ] Viết test round-trip qua API public:

```java
@Test void appendReadFlushAndReopen(@TempDir Path dir) throws Exception {
    var config = new LogConfig(4096, 1024, 64);
    try (var log = PartitionLog.open(dir, config)) {
        assertEquals(new AppendResult(0, 2), log.append(StorageFixtures.records("a", "b")));
        assertEquals(0, log.durableEndOffset());
        var batch = log.read(1, 1).getFirst();
        assertEquals(0, batch.baseOffset());
        assertEquals(2, batch.nextOffset());
        assertTrue(log.read(2, 100).isEmpty());
        assertEquals(2, log.flush());
    }
    try (var log = PartitionLog.open(dir, config)) {
        assertEquals(2, log.logEndOffset());
        assertEquals(2, log.durableEndOffset());
        assertEquals(StorageFixtures.records("a", "b"), log.read(0, 1024).getFirst().records());
    }
}
```

- [ ] Chạy `mvn "-Dtest=PartitionLogTest" test`; expected FAIL vì PartitionLog chưa có.
- [ ] Implement open: create directory, mở `.lock` bằng CREATE/WRITE, `tryLock()`; null/OverlappingFileLockException thành IOException rõ ràng. Giữ lock suốt lifecycle và không xóa `.lock` lúc close. Acquire lock trước scan/repair. Nếu recovery rỗng, tạo `%020d.log` base 0 và index rỗng rồi force data; mở recovered segments và publish OPEN sau tất cả thành công. Nếu open lỗi, đóng từng resource và release lock, preserve lỗi gốc/suppressed cleanup errors.
- [ ] Implement append trong write lock: ensure OPEN, encode tại logEndOffset trước mutation, rollover nếu batch không vừa. Force segment cũ trước khi tạo mới; cập nhật durableEndOffset cho prefix đã force. Ghi batch đủ rồi cập nhật index, end offset, AppendResult. Chỉ publish end khi mọi mutation bắt buộc thành công. Nếu lỗi sau mutation, chuyển FAILED. Cập nhật index file bằng write lại sparse entries (tối ưu incremental dành phase sau).

```text
writeLock.lock()
try:
  ensureOpen()
  bytes = BatchCodec.encode(logEndOffset, records, config.maxBatchBytes())
  if active.size + bytes.length > config.segmentBytes:
    active.force()
    durableEndOffset = logEndOffset
    create new active at logEndOffset
  position = active.append(bytes)
  index.consider(logEndOffset, position)
  index.write(indexPath, io)
  logEndOffset += records.size
  return [oldEnd, logEndOffset)
finally: writeLock.unlock()
```

- [ ] Implement read trong read lock: range/budget validation; floor segment rồi floor index, decode tuần tự bằng positional read. Bỏ batch có nextOffset <= requested offset. Trả batch đầu dù vượt budget; batch sau chỉ thêm nếu tổng encoded bytes không vượt budget (tính long chống overflow). Không quét từ byte 0 nếu index có entry phù hợp. Không trả buffer liên kết file. Corruption phát hiện trong read trả CorruptLogException; không sửa file trong read.
- [ ] Implement flush dưới write lock: force mọi segment trước khi tăng durableEndOffset = logEndOffset. close khỏe gọi cùng logic flush nội bộ mà không tạo vòng gọi close; close mọi resource và release lock kể cả force lỗi; close lần hai không làm gì. logStartOffset luôn 0 trong Phase 1, getters lock và ensure OPEN.
- [ ] Test rollover bằng config `(128, 128, 64)` và 3 batch một record 51 bytes: file base 0 chứa hai batch, file base 2 chứa một batch; read xuyên segment, budget 102 trả hai batch; budget 1 trả đúng một batch. Test đọc giữa batch, ngoài range, budget <= 0, config/batch invalid không tiêu offset, restart append tiếp, data vẫn dùng được sau close vì được copy, khóa hai lần cùng directory bị từ chối.
- [ ] Chạy `mvn "-Dtest=PartitionLogTest,LogRecoveryTest" test` rồi `mvn test`; expected PASS. Commit `feat: add partition log lifecycle and batched reads`.

## Task 7: Durability markers và trạng thái FAILED

**Files:** Modify M/PartitionLog.java, T/ScriptedLogIo.java. Create T/PartitionDurabilityTest.java.

**Interfaces:** Consumes overload `PartitionLog.open(Path, LogConfig, LogIo)` và ScriptedLogIo task 3. Không mở rộng public API. ScriptedLogIo chỉ điều khiển call cụ thể; fault được bật SAU open để không chặn recovery force.

- [ ] Viết test failure của flush:

```java
@Test void failedFlushPoisonsLogButReleasesLockOnClose(@TempDir Path dir) throws Exception {
    var io = new ScriptedLogIo();
    var config = new LogConfig(4096, 1024, 64);
    var log = PartitionLog.open(dir, config, io);
    log.append(StorageFixtures.records("a"));
    assertEquals(0, log.durableEndOffset());
    io.failForce = true;
    assertThrows(IOException.class, log::flush);
    assertThrows(IllegalStateException.class, () -> log.read(0, 1024));
    assertThrows(IllegalStateException.class, () -> log.append(StorageFixtures.records("b")));
    log.close(); // FAILED: cleanup only, no additional force attempt
    try (var reopened = PartitionLog.open(dir, config)) {
        assertEquals(1, reopened.logEndOffset());
    }
}
```

- [ ] Chạy `mvn "-Dtest=PartitionDurabilityTest" test`. Nếu case đã xanh do task 6, thêm case lỗi partial write/index write hoặc close-force chưa xử lý và xác nhận đỏ; không làm code sai cố ý.
- [ ] Implement một nơi ghi firstFailure và đổi FAILED; validation trước mutation không poison. Ghi/index/create/force lỗi sau mutation phải poison. CLOSED/FAILED operations ngoài close đều fail fast. close FAILED cleanup không thử flush; close OPEN mà flush lỗi vẫn throw lỗi đó sau khi đã cleanup. Cleanup nhiều lỗi dùng addSuppressed.

```java
private void markFailed(IOException error) {
    if (firstFailure == null) firstFailure = error;
    state = State.FAILED;
}
private void ensureOpen() {
    if (state != State.OPEN) throw new IllegalStateException("Log is " + state, firstFailure);
}
```

- [ ] Test append không force batch mới (reset counter sau open), rollover force prefix, flush group nhiều append, open force trước công bố recovered durable end. Test partial write ném lỗi, close FAILED rồi reopen chỉ giữ prefix hợp lệ; lỗi index write sau data hoàn chỉnh có thể khiến append không được báo thành công nhưng record vẫn hiện sau recovery, phải ghi rõ trong docs.
- [ ] Test close-force ném IOException nhưng reopen lấy được lock; open force failure cũng release lock; validation lỗi vẫn cho phép append đúng tiếp theo. Khi firstFailure có sẵn, giữ cause đầu tiên thay vì overwrite.
- [ ] Chạy `mvn "-Dtest=PartitionDurabilityTest,PartitionLogTest" test`; expected PASS. Commit `fix: enforce storage durability and failure boundaries`.

## Task 8: Truncate theo batch boundary

**Files:** Modify M/PartitionLog.java, T/ScriptedLogIo.java. Create T/PartitionTruncateTest.java.

**Interfaces:** Produces `void PartitionLog.truncateTo(long offset) throws IOException`. Consumes `LogIo.delete/truncate`, segment/index APIs. Thêm test-only `int failDeleteAt` default -1 và counter `deleteCalls` vào ScriptedLogIo; ném trước delete được chọn. Không thêm journaling hoặc đổi contract atomicity.

- [ ] Viết test đúng boundary và từ chối giữa batch:

```java
@Test void truncatesAtBatchBoundaryAndReusesTailOffsets(@TempDir Path dir) throws Exception {
    try (var log = PartitionLog.open(dir, new LogConfig(256, 128, 64))) {
        log.append(StorageFixtures.records("a", "b"));
        log.append(StorageFixtures.records("c"));
        assertThrows(IllegalArgumentException.class, () -> log.truncateTo(1));
        assertEquals(3, log.logEndOffset());
        log.truncateTo(2);
        assertEquals(2, log.logEndOffset());
        assertEquals(new AppendResult(2, 3), log.append(StorageFixtures.records("replacement")));
    }
}
```

- [ ] Chạy `mvn "-Dtest=PartitionTruncateTest" test`; expected FAIL vì chưa có method.
- [ ] Resolve boundary trước khi sửa bất kỳ file nào: offset thuộc [0, end], end là no-op, còn lại phải khớp batch.baseOffset. Dùng index floor rồi decode tìm chính xác file byte position, không round xuống.
- [ ] Giữ segment chứa boundary kể cả boundary == segment.base; xóa segment có base lớn hơn target từ cuối về đầu. Đóng handles trước delete trên Windows. Xóa data trước index; nếu chỉ còn orphan index khi crash, recovery bỏ qua/rebuild nó. Boundary 0 giữ file base 0 và truncate về 0.

```text
under write lock, ensure OPEN:
  resolve and validate exact batch boundary
  if offset == logEndOffset: return
  for segment with base > targetSegment.base, descending:
    close segment; delete data; delete index if present; remove from maps
  targetSegment.truncate(targetPosition)
  rebuild target index from retained batches
  force every retained data segment
  publish logEndOffset = durableEndOffset = offset
on IOException after mutation: mark FAILED, rethrow
```

- [ ] Test first/middle/last segment, zero, end no-op, outside range, between records; offset giữ lại không đổi. Inject delete/truncate failure sau một mutation, close, recover rồi retry target; không yêu cầu crash để lại toàn bộ trạng thái cũ hoặc mới. Nếu fault sau file delete nhưng trước map update, FAILED tránh phục vụ map cũ.
- [ ] Chạy `mvn "-Dtest=PartitionTruncateTest,LogRecoveryTest,PartitionDurabilityTest" test`; expected PASS. Commit `feat: support batch boundary log truncation`.

## Task 9: Concurrency và process crash tests

**Files:** Create T/PartitionConcurrencyTest.java, T/PartitionCrashTest.java, T/CrashWriter.java. Modify T/ScriptedLogIo.java nếu thêm hooks hữu ích.

**Interfaces:** Chỉ dùng public PartitionLog và LogIo seam. `CrashWriter.main(String[] args)` nhận directory và mode (`flushed`, `unflushed`, `partial`, `lock`). Giao tiếp với parent bằng stdout line `READY`; sau đó đợi stdin. Parent đóng/kill child trong finally.

- [ ] Viết concurrency test dùng latch, không dùng Thread.sleep để sắp xếp lịch. Pause write sau byte đầu của batch, reader báo đã bắt đầu rồi gọi read; reader phải chờ write lock. Sau khi release latch, reader thấy nguyên batch và mọi offset liên tiếp.

```java
CountDownLatch firstByteWritten = new CountDownLatch(1);
CountDownLatch releaseWrite = new CountDownLatch(1);
AtomicBoolean armed = new AtomicBoolean(false);
LogIo io = new LogIo() {
    @Override int write(FileChannel ch, ByteBuffer src, long pos) throws IOException {
        if (armed.compareAndSet(true, false)) {
            int limit = src.limit();
            src.limit(src.position() + 1);
            int n;
            try { n = super.write(ch, src, pos); } finally { src.limit(limit); }
            firstByteWritten.countDown();
            try {
                if (!releaseWrite.await(5, TimeUnit.SECONDS)) throw new IOException("Test timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return n;
        }
        return super.write(ch, src, pos);
    }
};
try (var log = PartitionLog.open(dir, new LogConfig(4096, 1024, 64), io)) {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
        armed.set(true);
        Future<AppendResult> writer = pool.submit(() -> log.append(StorageFixtures.records("a")));
        assertTrue(firstByteWritten.await(5, TimeUnit.SECONDS));
        CountDownLatch readerStarted = new CountDownLatch(1);
        Future<List<RecordBatch>> reader = pool.submit(() -> {
            readerStarted.countDown();
            return log.read(0, 1024);
        });
        assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> reader.get(100, TimeUnit.MILLISECONDS));
        releaseWrite.countDown();
        assertEquals(new AppendResult(0, 1), writer.get(5, TimeUnit.SECONDS));
        assertEquals(1, reader.get(5, TimeUnit.SECONDS).getFirst().nextOffset());
    } finally {
        releaseWrite.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
```

- [ ] Chạy `mvn "-Dtest=PartitionConcurrencyTest" test`; nếu invariant đã pass, giữ test làm regression và chỉ sửa lỗi thực tế. Thêm concurrent append từ 4 writers, mỗi writer 25 batch: kết quả unique, liên tiếp, không mất record; nhiều reader positional không tranh cursor. Future timeout hữu hạn, executor shutdown trong finally.
- [ ] Viết CrashWriter: dùng config `(4096, 1024, 64)`. Mode flushed append `a`, flush; mode unflushed append `a`, flush, append `b`; partial append/flush `a`, sau đó LogIo write đúng một byte của batch `b`, in READY/flush stdout rồi block trước khi ghi tiếp; lock chỉ open. Không dùng try-with-resources kết thúc trước parent kill.

```java
static void readyAndBlock() throws IOException {
    System.out.println("READY");
    System.out.flush();
    System.in.read();
}
```

- [ ] Implement test harness launch bằng ProcessBuilder argument list, không ghép shell string. Classpath chỉ cần main/test output lấy từ protection domain của PartitionLog và CrashWriter; không phụ thuộc JUnit trong child. Java executable lấy từ java.home/bin/java.exe trên Windows, java ở OS khác.

```java
String javaName = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
String java = Path.of(System.getProperty("java.home"), "bin", javaName).toString();
String cp = Path.of(PartitionLog.class.getProtectionDomain().getCodeSource().getLocation().toURI())
    + File.pathSeparator
    + Path.of(CrashWriter.class.getProtectionDomain().getCodeSource().getLocation().toURI());
Process child = new ProcessBuilder(java, "-cp", cp, CrashWriter.class.getName(),
    dir.toString(), "flushed").redirectError(ProcessBuilder.Redirect.INHERIT).start();
ExecutorService readerPool = Executors.newSingleThreadExecutor();
try {
    BufferedReader output = child.inputReader(StandardCharsets.UTF_8);
    Future<String> ready = readerPool.submit(output::readLine);
    assertEquals("READY", ready.get(10, TimeUnit.SECONDS));
    child.destroyForcibly();
    assertTrue(child.waitFor(10, TimeUnit.SECONDS));
    try (var log = PartitionLog.open(dir, new LogConfig(4096, 1024, 64))) {
        assertEquals(1, log.logEndOffset());
    }
} finally {
    if (child.isAlive()) child.destroyForcibly();
    assertTrue(child.waitFor(10, TimeUnit.SECONDS));
    try {
        child.getInputStream().close();
        child.getOutputStream().close();
        child.getErrorStream().close();
    } finally {
        readerPool.shutdownNow();
        assertTrue(readerPool.awaitTermination(5, TimeUnit.SECONDS));
    }
}
```

- [ ] Acceptance: flushed prefix luôn còn sau process kill; unflushed case chấp nhận end 1 hoặc 2 và xác minh payload/prefix, không đòi `b` mất; partial case chỉ `a` còn và cảnh báo recovery; mode lock từ chối open parent khi child giữ lock, cho open sau kill. Test subprocess executable/path có khoảng trắng. `@Timeout(30)` làm giới hạn ngoài cho từng test, vẫn có cleanup riêng.
- [ ] Chạy `mvn "-Dtest=PartitionConcurrencyTest,PartitionCrashTest" test` rồi `mvn test`; expected PASS. Không rerun tùy ý cho đến khi xanh để che flaky tests; lỗi cần phân tích nguyên nhân. Commit `test: verify concurrent access and process crash recovery`.

## Task 10: Ví dụ chạy được và tài liệu hợp đồng

**Files:** Create `src/main/java/vn/huyqt/logbroker/storage/example/StorageExample.java`, T/StorageExampleTest.java, `docs/storage-format-v1.md`. Modify README.md.

**Interfaces:** `StorageExample.main(String[] args) throws Exception`, package `vn.huyqt.logbroker.storage.example`; nhận đúng một đường dẫn thư mục trống hoặc chưa tồn tại. Từ chối directory có sẵn data để không sửa dữ liệu người dùng ngoài dự kiến. Ví dụ append hai records, read, flush, close, reopen.

- [ ] Viết smoke test hành vi example:

```java
@Test void exampleWritesReadableData(@TempDir Path root) throws Exception {
    Path dir = root.resolve("example");
    vn.huyqt.logbroker.storage.example.StorageExample.main(new String[]{dir.toString()});
    try (var log = PartitionLog.open(dir, LogConfig.defaults())) {
        assertEquals(2, log.logEndOffset());
        assertEquals(2, log.read(0, 1024).getFirst().records().size());
    }
}
```

- [ ] Chạy `mvn "-Dtest=StorageExampleTest" test`; expected FAIL vì example chưa có.
- [ ] Implement demo với public constructors, không dùng StorageFixtures từ test:

```java
List<LogRecord> records = List.of(
    new LogRecord(0, null, "hello".getBytes(StandardCharsets.UTF_8), List.of()),
    new LogRecord(1, null, "broker".getBytes(StandardCharsets.UTF_8), List.of()));
try (var log = PartitionLog.open(directory, LogConfig.defaults())) {
    System.out.println(log.append(records));
    System.out.println("batches=" + log.read(0, 1024).size());
    System.out.println("durableEndOffset=" + log.flush());
}
try (var log = PartitionLog.open(directory, LogConfig.defaults())) {
    System.out.println("recoveredEndOffset=" + log.logEndOffset());
}
```

- [ ] Viết docs với byte table task 2, null/empty distinction, read trả whole batch, return budget, [first,next) offset, state OPEN/FAILED/CLOSED, recovery decision table, truncate boundary, config constraints. Nêu append bị lỗi vẫn có thể hiện sau restart nếu data đã ghi đủ; durableEndOffset không phải high watermark. Rollover có thể force dù append không hứa durability. close khỏe flush; close FAILED không flush. maxBatchBytes lúc reopen phải đủ cho dữ liệu hiện có.
- [ ] Ghi rõ giới hạn directory-entry persistence trên filesystem/OS và việc process kill không mô phỏng power loss. Giải thích index có thể rebuild, không mất message vì index hỏng, nhưng index I/O failure vẫn phải báo khi không phục vụ được.
- [ ] README cập nhật Java 21/Maven 3.9.x, liên kết format/spec/plan, lệnh build và chạy từ repo root:

```powershell
mvn clean verify
java -cp target/classes vn.huyqt.logbroker.storage.example.StorageExample target/example-log
```

- [ ] Chạy `mvn clean verify`, expected BUILD SUCCESS với mọi test, không có skipped do thiếu dependency/môi trường. Chạy example ở thư mục demo mới; expected AppendResult [0,2), durableEndOffset=2 và recoveredEndOffset=2. Không xóa thư mục tùy ý để chạy lại; dùng tên demo mới hoặc thư mục temporary xác định.
- [ ] Chạy `git diff --check`; xem `git status --short` bảo đảm không stage data/runtime output. Commit `docs: document storage format and runnable example` cùng example/test sau khi test qua.

## Ma trận coverage spec → task

| Yêu cầu spec | Task / bằng chứng |
| --- | --- |
| Model/batch format, null/rỗng/headers | 1–2, model tests và byte fixture |
| CRC, length bounds, version, overflow | 2, malformed input cases |
| Segment rollover và sparse index | 3–4, 6, lookup và đọc xuyên segment |
| Append/read/flush/open/close | 6–7, round-trip và failure tests |
| One writer/multiple readers, directory lock | 6, 9, latches và process lock |
| Durable marker, write/force errors | 7, scripted short/failed I/O |
| Strict recovery, không sửa corruption | 5, data byte snapshots và mỗi vị trí cắt |
| Index rebuild | 4–5, thiếu/rác/truncated index |
| Batch-boundary truncate, retry sau gián đoạn | 8, mutation failpoints |
| Process crash | 9, child READY/kill/reopen |
| Ownership dữ liệu/API độc lập channel | 1, 6, defensive-copy và after-close tests |
| Ví dụ và giới hạn durability | 10, smoke test + docs |

## Checkpoint và phạm vi giao việc

1. Sau task 2: review binary format/validation trước khi ghi file thực tế.
2. Sau task 5: review strict recovery; lỗi data không được biến thành success bằng truncate.
3. Sau task 8: review hợp đồng API, durability và truncate; chạy toàn suite.
4. Sau task 10: bàn giao storage library, kết quả test và các giới hạn đã biết; chưa chuyển sang networking.

Nếu dùng agent, giao từng task theo dependency, đọc spec + contract chung + task liên quan; không chạy song song các task sửa PartitionLog. Không tự đổi spec hoặc thêm phase khác. Nếu phát hiện cần đổi hợp đồng, ghi rõ khác biệt và xin review trước thay đổi.

## Tự review plan

- Coverage từng phần spec đã được ánh xạ trong bảng trên.
- Model/type/signature dùng thống nhất; test helpers được tạo trước nơi sử dụng.
- Không có implementation đã chạy trong lúc viết plan; các kết quả PASS ở task là kỳ vọng, không phải báo cáo hiện trạng.
- Hợp đồng chính vẫn giữ: append/flush tách biệt, batch v1, strict recovery, storage độc lập consumer.

## Tài liệu kỹ thuật đối chiếu

- [Java 21 FileChannel](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html): positional I/O, force và file locking.
- [JUnit 5.11.4 User Guide](https://docs.junit.org/5.11.4/user-guide/index.html): JUnit Jupiter, parameterized tests và temporary directories.

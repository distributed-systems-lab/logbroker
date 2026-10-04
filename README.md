# Java Log Broker

Dự án học distributed systems bằng cách tự xây distributed log/message broker bằng Java, theo các nguyên lý kiến trúc Kafka thế hệ KRaft.

Tự xây storage, replication, metadata consensus, protocol và Java client. Bắt đầu với persistent partition log; bổ sung Share Groups sau khi hoàn thiện replication, consumer groups và retention.

## Tài liệu

- [Lộ trình 9 phase](docs/superpowers/specs/2026-09-24-broker-roadmap.md)
- [Thiết kế Phase 1 — Persistent partition log](docs/superpowers/specs/2026-09-24-storage-phase-1-design.md)
- [Implementation plan — Phase 1](docs/superpowers/plans/2026-09-25-storage-phase-1.md)
- [Thiết kế Phase 2 — Broker đơn và Java client](docs/superpowers/specs/2026-09-25-broker-phase-2-design.md)
- [Implementation plan — Phase 2](docs/superpowers/plans/2026-09-25-broker-phase-2.md)
- [Thiết kế Phase 3 — Metadata quorum](docs/superpowers/specs/2026-09-28-metadata-quorum-phase-3-design.md)
- [Implementation plan — Phase 3](docs/superpowers/plans/2026-09-28-metadata-quorum-phase-3.md)
- [Thiết kế Phase 4 — Cluster và quản lý partition](docs/superpowers/specs/2026-10-02-cluster-phase-4-design.md)
- [Implementation plan — Phase 4](docs/superpowers/plans/2026-10-03-cluster-phase-4.md)
- [Storage format v1 và hợp đồng API](docs/storage-format-v1.md)
- [Wire protocol v1](docs/protocol-v1.md)
- [Broker configuration](docs/broker-configuration.md)

## Trạng thái

Phase 1 đã có thư viện persistent partition log và bộ kiểm thử cho codec, recovery, durability, truncate, concurrency và process crash.

Phase 2 có broker một node, wire protocol version 1, Netty TCP transport, Java client, CreateTopic, Metadata, Produce APPENDED/FLUSHED và Fetch theo offset. Metadata và partition log được khôi phục từ đĩa; các bài kiểm thử bao gồm restart, crash process và mất phản hồi mạng.

## Yêu cầu và chạy thử

- Java 21
- Maven 3.9.x

Chạy từ thư mục gốc repository:

```powershell
mvn clean verify
java -cp target/classes vn.huyqt.logbroker.storage.example.StorageExample target/example-log
```

Format toàn bộ Java source và test bằng Spotless (AOSP, indent 4 spaces):

```powershell
mvn spotless:apply
mvn spotless:check
# Script tự tìm thư mục gốc repository:
powershell -NoProfile -File scripts/format.ps1 apply
powershell -NoProfile -File scripts/format.ps1 check
```

Trên Bash dùng `bash scripts/format.sh apply` hoặc `bash scripts/format.sh check`.
`check` chỉ kiểm tra, không sửa file; `mvn verify` cũng chạy bước này. Formatter
và phiên bản được cố định trong `pom.xml`, chuẩn hóa import, bỏ import không dùng,
giữ nguyên string literals và dùng line ending LF. Chỉ áp dụng cho
`src/main/java/**/*.java` và `src/test/java/**/*.java`,
không format output trong `target/`. Lần đầu cần Maven tải plugin/dependencies.

Chạy demo cluster trên Linux/WSL ext4:

```bash
mvn clean verify dependency:copy-dependencies
bash scripts/cluster-demo.sh /tmp/logbroker-demo-new
```

Phase 4 production requires explicit formatting and cluster configuration. Use Linux/WSL ext4 for strict controller durability. The example bootstraps from one broker and prints `SUCCESS records=6 brokers=3` after verifying all six partitions. See [cluster setup](docs/cluster-configuration.md), [operations](docs/cluster-operation.md), and [verification](docs/cluster-verification.md). PowerShell classpaths use `;`.

Phase 3 implements a fixed three-voter KRaft-style metadata quorum with durable elections, replication, snapshots, a separate admin client and CLI. Strict controllers run on Linux/WSL. See [configuration and three-node commands](docs/controller-configuration.md) and [operation contract](docs/controller-operation.md). Phase 4 adds broker observers, durable partition provisioning and cluster routing; final acceptance is tracked separately.


Phase 3 acceptance passed on WSL/ext4: 262 tests with no failures, errors or skips, real three-process crash/restart/snapshot catch-up, and production CLI smoke. See [verification evidence and limitations](docs/controller-verification.md).

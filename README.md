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

Chạy broker và client ví dụ trong hai terminal PowerShell:

```powershell
mvn clean verify dependency:copy-dependencies
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.broker.BrokerMain --data target/broker-data --port 9092
java -cp "target/classes;target/dependency/*" vn.huyqt.logbroker.example.ClientExample 127.0.0.1 9092 demo
```

Broker có thể nhận thêm `--config broker.properties`; xem [bảng cấu hình](docs/broker-configuration.md). Dừng broker bằng Ctrl+C. Ví dụ client in `SUCCESS records=1` sau khi Produce FLUSHED và Fetch lại đúng record.

Ví dụ chỉ chấp nhận thư mục chưa tồn tại hoặc đang rỗng. Dùng tên thư mục mới nếu chạy lại.

Phase 3 implements a fixed three-voter KRaft-style metadata quorum with durable elections, replication, snapshots, a separate admin client and CLI. Strict controllers run on Linux/WSL. See [configuration and three-node commands](docs/controller-configuration.md) and [operation contract](docs/controller-operation.md). Broker/partition provisioning remains a later phase.


Phase 3 acceptance passed on WSL/ext4: 252 tests with no failures or skips, real three-process crash/restart/snapshot catch-up, and production CLI smoke. See [verification evidence and limitations](docs/controller-verification.md).


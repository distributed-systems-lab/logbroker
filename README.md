# Java Log Broker

Dự án học distributed systems bằng cách tự xây distributed log/message broker bằng Java, theo các nguyên lý kiến trúc Kafka thế hệ KRaft.

Tự xây storage, replication, metadata consensus, protocol và Java client. Bắt đầu với persistent partition log; bổ sung Share Groups sau khi hoàn thiện replication, consumer groups và retention.

## Tài liệu

- [Lộ trình 9 phase](docs/superpowers/specs/2026-09-24-broker-roadmap.md)
- [Thiết kế Phase 1 — Persistent partition log](docs/superpowers/specs/2026-09-24-storage-phase-1-design.md)
- [Implementation plan — Phase 1](docs/superpowers/plans/2026-09-25-storage-phase-1.md)
- [Thiết kế Phase 2 — Broker đơn và Java client (chờ review)](docs/superpowers/specs/2026-09-25-broker-phase-2-design.md)
- [Storage format v1 và hợp đồng API](docs/storage-format-v1.md)

## Trạng thái

Phase 1 đã có thư viện persistent partition log và bộ kiểm thử cho codec, recovery, durability, truncate, concurrency và process crash.

Phase 2 đã có bản spec tổng hợp các quyết định kiến trúc, đang chờ review trước khi lập implementation plan.

## Yêu cầu và chạy thử

- Java 21
- Maven 3.9.x

Chạy từ thư mục gốc repository:

```powershell
mvn clean verify
java -cp target/classes vn.huyqt.logbroker.storage.example.StorageExample target/example-log
```

Ví dụ chỉ chấp nhận thư mục chưa tồn tại hoặc đang rỗng. Dùng tên thư mục mới nếu chạy lại.

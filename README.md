# Java Log Broker

Dự án học distributed systems bằng cách tự xây distributed log/message broker bằng Java, theo các nguyên lý kiến trúc Kafka thế hệ KRaft.

Tự xây storage, replication, metadata consensus, protocol và Java client. Bắt đầu với persistent partition log; bổ sung Share Groups sau khi hoàn thiện replication, consumer groups và retention.

## Tài liệu

- [Lộ trình 9 phase](docs/superpowers/specs/2026-09-24-broker-roadmap.md)
- [Thiết kế Phase 1 — Persistent partition log](docs/superpowers/specs/2026-09-24-storage-phase-1-design.md)

## Trạng thái

Thiết kế Phase 1 đã được duyệt. Bước tiếp theo là lập kế hoạch triển khai. Chưa có mã nguồn hoặc cấu hình build.

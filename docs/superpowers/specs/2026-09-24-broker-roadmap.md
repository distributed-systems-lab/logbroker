# Lộ trình xây dựng distributed log / message broker

Ngày: 2026-09-24

Trạng thái: Người dùng đã đồng ý lộ trình; từng phase cần thiết kế chi tiết trước khi triển khai.

## Mục tiêu và quyết định

- Học sâu distributed systems bằng cách tự xây broker từ đầu bằng Java.
- Giữ nguyên các nguyên lý kiến trúc Kafka thế hệ KRaft; giảm phạm vi tính năng theo từng phase.
- Tự viết protocol và Java client; chưa yêu cầu tương thích Kafka client.
- Cập nhật ngày 2026-09-28: mục tiêu dài hạn là tương đương chức năng Kafka, hoàn thiện dần qua các phase, vẫn dùng protocol/client riêng.
- Tự viết storage, metadata consensus, partition replication và consumer coordination.
- Dùng thư viện hỗ trợ qua ranh giới rõ ràng. Ban đầu dự kiến dùng Netty; có thể bổ sung Java NIO transport sau bằng cùng contract.
- Không để kiểu dữ liệu của thư viện networking lan vào lõi storage hoặc replication.
- Đánh giá tối ưu bằng benchmark; chuyển sang standard library không tự động đồng nghĩa với tăng hiệu năng.
- Metadata quorum dùng Raft; replication dữ liệu partition dùng leader/follower, ISR và high watermark. Không dùng một nhóm Raft cho mỗi data partition.
- Chuẩn bị ranh giới cho Share Groups từ đầu; triển khai sau retention và trước tối ưu hiệu năng.

## Ranh giới để bổ sung Share Groups

- Phase 1: storage không biết consumer/group; không ghi ACK, owner hay acquisition lock vào record gốc. Đọc theo offset độc lập, không có con trỏ đọc chung. Offset của record còn giữ lại không thay đổi; replication có thể truncate phần đuôi chưa commit theo thiết kế Phase 5.
- Phase 2: protocol phân biệt operation và version; bổ sung ShareFetch/ShareAcknowledge sau mà không đổi nghĩa Fetch/CommitOffset.
- Phase 2: topic có ID ổn định, đưa lên sớm từ mốc Phase 4 theo thảo luận spec Phase 2; tạo nền tảng phân biệt các lần xóa/tạo lại cùng tên khi bổ sung lifecycle đó.
- Phase 6: tách membership/assignment khỏi mô hình tiến độ; không buộc mọi group dùng một committed offset cho mỗi partition.
- Phase 7: retention độc lập với ACK; thiết kế hành vi khi dữ liệu chưa xử lý đã hết retention.
- Share state tách khỏi data log và metadata quorum KRaft. Không tạo framework mở rộng tổng quát trước khi có nhu cầu thực tế.

## Các phase

### 1. Persistent log trên một máy

Record format có version, length và checksum; append-only log; offset; segment rollover; sparse offset index; đọc theo offset; recovery sau crash; hợp đồng durability rõ ràng.

Điều kiện hoàn thành: đóng/mở lại đọc đúng dữ liệu; kiểm thử ghi dở, checksum lỗi, index recovery và rollover. Chưa có networking.

Quyết định đã chốt: tách append() và flush(). append() thành công chưa cam kết dữ liệu bền vững trước mất điện; flush() là ranh giới đồng bộ dữ liệu xuống thiết bị lưu trữ. Có thể gom nhiều lần append trước một lần flush. Đặc tả Phase 1 sẽ xác định phạm vi offset được flush, xử lý lỗi I/O và giả định filesystem/thiết bị.

Quyết định đã chốt: lưu theo record batch ngay từ Phase 1. Định dạng chi tiết và chính sách recovery sẽ được chốt trong đặc tả phase; chưa yêu cầu tương thích binary format của Kafka.

Quyết định đã chốt: recovery nghiêm ngặt. Chỉ tự cắt batch cuối bị thiếu byte trong active segment khi cấu trúc phần còn đọc được hợp lệ. Batch đủ byte nhưng checksum sai, header không hợp lệ hoặc segment cũ hỏng phải làm quá trình mở partition thất bại; không tự bỏ qua dữ liệu hỏng.

### 2. Broker đơn và Java client

Topic/partition; binary protocol có version, request ID, mã lỗi, giới hạn kích thước; create topic, metadata, produce, fetch; Netty transport; batching và backpressure cơ bản. Consumer tự chọn partition và offset.

Thiết kế chi tiết: [Phase 2 — Broker đơn và Java client](2026-09-25-broker-phase-2-design.md), đã duyệt ngày 2026-09-25; [implementation plan](../plans/2026-09-25-broker-phase-2.md) đã viết. Các quyết định đã chốt gồm APPENDED/FLUSHED, flush theo thời gian hoặc byte, Fetch đến logEndOffset, long polling, request nhiều partition, local metadata log và topic ID từ Phase 2.

Điều kiện hoàn thành: nhiều client ghi/đọc được; restart phục hồi theo cam kết durability đã chọn.

### 3. Metadata quorum theo KRaft

Ba controller với membership cố định; bầu leader; epoch; replicate/commit metadata log; recovery và snapshot. Có thể tái sử dụng storage nhưng tách riêng logic consensus.

Thiết kế chi tiết: [Phase 3 — Metadata quorum](2026-09-28-metadata-quorum-phase-3-design.md), bản tổng hợp chờ review. Quorum độc lập với broker Phase 2; tái sử dụng storage qua QuorumLog, commit dựa trên đa số đã flush và quy tắc epoch, đọc linearizable qua log barrier, snapshot/catch-up và format tường minh.

Điều kiện hoàn thành: metadata đã commit thống nhất; phía thiểu số không commit được thay đổi; kiểm thử mất leader, mất quorum và network partition.

### 4. Cluster và quản lý partition

Broker registration, heartbeat, fencing; topic/replica assignment/partition leader metadata; broker áp dụng metadata; client routing và refresh. Bước đầu replication factor bằng một.

Điều kiện hoàn thành: ghi/đọc đúng partition trên nhiều broker; restart không làm mất metadata đã commit. Chưa cam kết data availability khi broker chứa partition ngừng hoạt động.

### 5. Replication và failover dữ liệu

Follower fetch; ISR; high watermark; leader epoch; acknowledgment modes; min.insync.replicas; controller chọn leader hợp lệ; xử lý divergent log và replica rejoin; consumer đọc dữ liệu đã commit.

Điều kiện hoàn thành: ba broker, replication factor ba vượt qua crash/network partition/rejoin tests; xác minh bảo đảm dữ liệu theo từng acknowledgment mode và giả định lỗi được thiết kế rõ.

### 6. Consumer groups và offset management

Group coordinator; join/leave/heartbeat/rebalance; partition assignment; committed offsets trong internal topic; coordinator recovery; Java client hỗ trợ at-least-once.

Điều kiện hoàn thành: phân công lại khi consumer thay đổi; resume từ committed offset; mô tả rõ khả năng xử lý trùng.

### 7. Retention và kiểm chứng toàn hệ thống

Retention theo thời gian/dung lượng; xóa segment an toàn; offset hết hạn; metrics; log chẩn đoán; CLI; fault injection cho crash, delay, mất kết nối và lỗi I/O.

Điều kiện hoàn thành: kịch bản lỗi lặp lại được và báo cáo các bảo đảm đáp ứng. Kiểm thử lỗi của từng phase vẫn phải thực hiện ngay tại phase đó.

### 8. Share Groups và ShareConsumer

Cho phép nhiều consumer trong cùng group xử lý các record khác nhau của cùng partition. Quản lý trạng thái riêng theo (group, topicId, partition), hỗ trợ ACK không liên tục bằng trạng thái record/khoảng offset; acquisition lock có thời hạn; accept/release/reject; giới hạn record đang xử lý và delivery attempts.

Tách membership/assignment, quản lý giao record tại partition leader và persistence của share state trong internal topic riêng. Phân biệt trạng thái tạm thời của lần giao với trạng thái bền vững phải phục hồi. Hợp đồng xử lý ACK phải xác định thời điểm persistence hoàn tất trước khi báo thành công.

Điều kiện hoàn thành: kiểm thử ACK đến muộn sau khi giao lại, ACK retry khi mất phản hồi, consumer crash, broker/coordinator failover, giới hạn in-flight, delivery attempts và retention đồng thời. Xác minh group độc lập, khả năng giao lại và không cam kết thứ tự hoàn tất xử lý theo partition hoặc exactly-once. Client có thể dùng số consumer lớn hơn số partition.

Chi tiết acquisition renewal và chính sách hết số lần giao sẽ được chốt khi thiết kế phase, không cần triển khai trong storage.

### 9. Đo hiệu năng và thay thế thư viện có chọn lọc

Benchmark throughput, latency, allocation, disk/network I/O; batching và buffer reuse; Java NIO transport cùng contract với Netty; dùng chung conformance tests và benchmark.

Điều kiện hoàn thành: có dữ liệu so sánh để quyết định giữ/thay từng thành phần.

## Phần mở rộng sau lộ trình đầu

Transactions/exactly-once, log compaction, tiered storage và thay đổi động thành viên controller quorum sẽ được bổ sung bằng các phase tiếp theo để tiến tới tương đương chức năng Kafka. Các phần security, rolling upgrade và recovery khi thay thế controller mất storage cũng cần spec riêng. Danh sách này là định hướng mở rộng, chưa phải danh mục đầy đủ hoặc thiết kế đã duyệt cho từng tính năng.

Kafka protocol compatibility không thuộc mục tiêu: dự án tiếp tục dùng protocol/client riêng. Các mốc mở rộng không làm tăng phạm vi triển khai Phase 3.

## Cách triển khai

Thiết kế, review và lập kế hoạch cho từng phase trước khi viết code. Bắt đầu bằng Phase 1. Các phase phía sau là định hướng, chưa phải đặc tả triển khai được phê duyệt.

## Tài liệu tham chiếu cho Share Groups

- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255070434/KIP-932%2BQueues%2Bfor%2BKafka)
- [KafkaShareConsumer API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaShareConsumer.html)

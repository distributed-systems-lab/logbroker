# Phase 3 — Metadata quorum theo KRaft

Ngày: 2026-09-28

Trạng thái: Bản spec tổng hợp đã được người dùng duyệt ngày 2026-09-28, bao gồm cấu hình và giới hạn ban đầu. Đã có [implementation plan](../plans/2026-09-28-metadata-quorum-phase-3.md). Chưa triển khai Phase 3.

## 1. Mục tiêu và phạm vi

Xây metadata quorum độc lập gồm ba controller với membership cố định. Quorum áp dụng metadata thật, bắt đầu bằng TopicCreated và TopicCatalog; không mở data partition. Broker và client Phase 2 tiếp tục hoạt động độc lập. Phase 4 sẽ kết nối broker với quorum, bổ sung registration, fencing, assignment và routing.

Mục tiêu dài hạn là tương đương chức năng Kafka theo từng phase, sử dụng protocol/client riêng. Không yêu cầu tương thích Kafka wire protocol hoặc binary format. Phase này theo nguyên lý KRaft: consensus cho metadata, follower chủ động Fetch; không tạo Raft group cho từng data partition.

Bao gồm election, epoch/vote bền vững, replication, commit, đọc linearizable, snapshot, truyền snapshot, thu gọn log, restart, CLI và fault tests. Membership động, thay thế voter mất toàn bộ storage, rolling upgrade, TLS/authentication và tích hợp broker nằm ngoài Phase 3. Những chức năng này không bị loại khỏi mục tiêu dài hạn.

## 2. Mô hình lỗi và bảo đảm

- Mạng có thể ngắt, trễ, trùng và đổi thứ tự phản hồi; process có thể crash/restart. Không hỗ trợ Byzantine nodes.
- Một voter không được chạy đồng thời trên hai process/storage khác nhau. Khóa độc quyền data directory trong suốt thời gian node hoạt động.
- Flush thành công được giả định bảo đảm persistence theo contract filesystem/thiết bị. Mất vĩnh viễn đa số storage không nằm trong bảo đảm recovery.
- Safety không phụ thuộc thời hạn RPC chính xác. Liveness cần đa số khỏe mạnh và mạng/đĩa cuối cùng đáp ứng đủ nhanh.
- Metadata đã báo thành công tồn tại sau restart cả cụm, nếu storage giữ đúng contract và danh tính voter không bị tái sử dụng sai.
- Thiểu số không commit thêm. Node bị cô lập không hoàn thành một đọc linearizable mới; local view vẫn có thể đọc để chẩn đoán.
- Không coi việc append vào page cache là bằng chứng durable để commit metadata.

## 3. Kiến trúc và ownership

| Thành phần | Trách nhiệm |
|---|---|
| QuorumStateMachine | Role, epoch, election, tiến độ replica, reconciliation và commit; độc lập Netty/disk implementation |
| QuorumLog | Adapter trên storage Phase 1; entry epoch, batch boundary, append/read/truncate và tích hợp snapshot |
| QuorumStateStore | Danh tính, epoch/vote bền vững và checkpoint cần cho recovery |
| MetadataStateMachine | Apply metadata đã commit, TopicCatalog, encode/decode metadata snapshot |
| ControllerService | Validate lệnh, pending CreateTopic, read barrier, hoàn thành request |
| SnapshotStore | Tạo, truyền, kiểm tra, công bố và cài đặt snapshot |
| QuorumTransport | RPC controller có giới hạn tài nguyên; implementation Netty |
| Admin client/CLI | Format, CreateTopic, đọc metadata và DescribeQuorum |

Một event loop sở hữu mọi trạng thái consensus và pending request. Mỗi node có hàng đợi disk I/O có thứ tự. Netty giải mã/kiểm tra envelope rồi đưa event vào event loop; không sửa trạng thái consensus hoặc thực hiện disk I/O trên network event loop.

Mỗi disk completion mang epoch, operation ID và log generation. Completion cũ vẫn phải được xử lý về tài nguyên và sự kiện ghi đĩa đã xảy ra, nhưng không được tạo vote, ACK hoặc quyết định commit dựa trên trạng thái cũ. Truncate, append, checkpoint và chuyển generation phải được sắp thứ tự; không có append cũ chạy sau truncate mới.

Không chặn event loop khi chờ I/O hoặc quorum. Encode snapshot từ một image bất biến, có giới hạn kích thước; việc công bố/cài đặt được tuần tự hóa với log mutation. Thay transport sau này không đổi consensus contract.

## 4. Danh tính và bootstrap

CLI format nhận clusterId (UUID), nodeId và danh sách đúng ba voter ID/endpoint duy nhất. Cluster ID phải giống nhau trên ba node. Ghi storage format version, membership cố định và danh tính vào manifest có checksum, lưu bền vững trước khi báo thành công.

Format chỉ nhận thư mục chưa tồn tại hoặc rỗng, không ghi đè directory có dữ liệu. Startup không tự format, đối chiếu manifest với cấu hình và từ chối sai cluster/node/membership/version. Phase này thay endpoint hoặc voter set yêu cầu thiết kế migration riêng; không chỉnh cấu hình để ngầm đổi membership.

Mọi RPC nội bộ chứa clusterId, senderId, protocol version và requestId; kiểm tra voter set khi thiết lập giao tiếp. Sai danh tính không làm thay đổi epoch/vote. Đây là kiểm tra cấu hình, không phải authentication. Admin có thể biết cluster ID qua cấu hình/bootstrap và phải kiểm tra cluster đích.

## 5. Log, offset và storage

Tái sử dụng storage Phase 1 qua QuorumLog, không đưa election vào PartitionLog. Record metadata/control có version, loại và epoch. Epoch của log không giảm theo thứ tự offset. Control record gồm LeaderChange và ReadBarrier; chúng chiếm offset nhưng không tạo topic.

Mọi mốc offset dùng quy ước exclusive: mốc N bao phủ các record có offset nhỏ hơn N. Các mốc append, durable, commit, apply và snapshot đều nằm trên batch boundary. Một batch không trộn epoch; replica phải giữ nguyên offset, nội dung và batch boundary của leader, không tự regroup khi append replica.

Trong một log generation hợp lệ:

`snapshotEndOffset <= appliedOffset <= commitOffset <= durableEndOffset <= logEndOffset`

`logStartOffset <= snapshotEndOffset`; phần còn giữ trước snapshot có thể phục vụ recovery. Khi mới format, tất cả mốc bằng 0. Sau truncate, logEnd/durable có thể giảm nhưng không xuống dưới committed prefix. Epoch trong log phải được đối chiếu với durable quorum state; nếu crash để lại log epoch cao hơn state được phép dùng để phục hồi, node phải nâng và persist epoch trước khi giao tiếp, không chạy với epoch thấp hơn log.

Storage cần primitive cụ thể để: mở log tại offset khác 0 sau snapshot, append replica giữ offset/boundary, xóa prefix theo segment và chuyển sang log generation mới an toàn. Không đổi binary format data log Phase 1 nếu chỉ cần encode metadata trong payload. Bất kỳ thay đổi format thực sự nào cần version riêng và compatibility test cho Phase 1/2.

Epoch lookup được dựng từ log và snapshot boundary; index/checkpoint phụ trợ có thể rebuild nhưng không được suy đoán entry đã commit từ việc nó còn trên đĩa. Truncate chỉ tại batch boundary, bảo toàn committed prefix và epoch lookup tương ứng.

## 6. Election và epoch

Node có trạng thái unattached, follower, candidate, leader hoặc failed/stopping. Restart bắt đầu chưa biết leader. Candidate tăng epoch, ghi bền vững epoch và self-vote trước khi gửi Vote.

Chỉ cấp một vote cho một candidate trong một epoch, kể cả qua restart. Vote yêu cầu log candidate ít nhất cập nhật bằng local log: so sánh epoch cuối rồi exclusive end offset, tính cả boundary snapshot. Candidate quảng bá durable log prefix; mọi pending append/truncate phải được giải quyết trước khi chốt log position để tham gia election.

Thông tin epoch cao hơn từ peer hợp lệ làm node rời role cũ, persist epoch mới (và vote nếu có) trước phản hồi phụ thuộc epoch đó. Vote đã lưu không được xóa rồi bỏ phiếu lần hai trong cùng epoch. Request/response cũ không được khôi phục role hoặc tiến độ cũ.

Được 2/3 phiếu thì trở thành leader, gửi BeginQuorumEpoch và append LeaderChange trong epoch mới. Leader chưa sẵn sàng phục vụ thao tác quản trị cho đến khi record thuộc epoch mới này commit và apply. EndQuorumEpoch là tín hiệu hỗ trợ bước xuống, không bắt buộc để failover thành công. Không dùng pre-vote trong Phase 3; test phải thể hiện election vẫn an toàn dù có thể tăng epoch khi mạng chập chờn.

Election timeout/backoff có jitter, clock monotonic. Leader theo dõi liên lạc mới từ đa số trong epoch hiện tại và bước xuống khi quá hạn; liên lạc cũ/replayed không gia hạn quyền leader. Bước xuống không tự chứng minh request đang chờ chưa commit.

## 7. Fetch replication và commit

Follower gửi Fetch gồm leader epoch đang biết, next offset và epoch của prefix cuối đã flush. Leader trả batch liên tiếp, commit offset hiện biết, hoặc chỉ dẫn reconciliation/snapshot. Follower append đúng offset, flush rồi mới quảng bá tiến độ đó trong Fetch tiếp theo. Tiến độ nhận/append chưa flush không được tính vào durable match.

Leader chỉ ghi nhận durable match sau khi kiểm tra prefix khớp epoch/offset của log hiện tại. Reset tiến độ peer khi đổi epoch; không lấy một offset đơn lẻ làm bằng chứng rằng nội dung khớp. Giới hạn một replication Fetch đang chờ cho mỗi follower; phản hồi phải khớp request và generation.

Reconciliation tìm prefix chung bằng epoch và end offset của epoch; mỗi bước phải tiến triển hoặc báo lỗi. Follower truncate phần đuôi khác nhau chưa commit rồi fetch lại. Mâu thuẫn đụng vào committed prefix là lỗi an toàn: node dừng tham gia quorum, không tự chữa bằng cách xóa committed data. Nếu prefix cần thiết đã bị thu gọn, leader chuyển follower sang snapshot hợp lệ.

Leader có thể nâng commit tới boundary N khi local durableEnd >= N, ít nhất 2/3 voter có durable matching prefix tới N, và record ngay trước N thuộc epoch leader hiện tại. Việc này commit cả prefix kế thừa trước N. Không dùng majority của entry epoch cũ để bỏ qua quy tắc epoch hiện tại.

Follower nâng commit tới min(leaderCommit, local durable matching end), chỉ từ phản hồi leader hợp lệ của epoch hiện tại và không giảm mốc đã biết. Commit/apply không vượt qua batch chưa hoàn chỉnh. Mốc commit được checkpoint sau dữ liệu tương ứng; checkpoint có thể chậm hơn memory commit. Crash mất checkpoint mới chỉ làm chậm việc tái xác nhận commit, không cho phép election làm mất committed entry.

MetadataStateMachine apply theo thứ tự, không apply speculative suffix. Apply lỗi do record không hợp lệ khiến node failed; không bỏ qua record. Leader hoàn thành CreateTopic sau commit và apply, không cần chờ mọi follower apply.

## 8. Lệnh metadata và đọc

CreateTopic dùng quy tắc tên và giới hạn topic/partition của Phase 2. TopicCreated chứa UUID khác 0, tên và số partition. Chưa có assignment, delete hoặc tăng partition. UUID không thay đổi qua retry, replay hoặc snapshot.

- Cùng tên và số partition đã tồn tại: trả cùng UUID.
- Cùng tên nhưng số partition khác: trả TOPIC_ALREADY_EXISTS với thông tin xung đột.
- Cùng tên đang chờ: request cùng tham số dùng chung thao tác; khác tham số trả xung đột. Admission capacity phải tính cả committed và pending topics/partitions.
- Leader mới hoàn tất LeaderChange và replay committed prefix trước admission; mọi entry được kế thừa trước marker lúc đó đã được giải quyết bằng commit/reconciliation.
- Timeout hoặc mất kết nối sau admission có thể có kết quả UNKNOWN. Không xóa entry khỏi log hoặc hoàn tác thao tác vì client hết chờ.

Admin API có hai kiểu đọc rõ ràng:

1. Linearizable metadata read: leader append ReadBarrier sau khi nhận request, đợi commit và apply tới barrier rồi chụp catalog bất biến. Chỉ các request đến trước append được dùng chung barrier. Nếu đổi epoch trước khi hoàn thành, hủy waiter với lỗi retryable; client có thể retry. Không dùng lease dựa trên wall clock.
2. Local metadata view: đọc catalog đã apply tại node, trả consistency=LOCAL, nodeId, epoch, knownLeader, commit/applied offsets. Có thể cũ; không tự nâng cấp thành linearizable khi node nghĩ mình là leader.

DescribeQuorum trả role và tiến độ mà node hiện biết; mô tả rõ peer progress không phải ảnh chụp đồng thời toàn cluster. Node failed có thể cung cấp status và local image cuối hợp lệ qua đường chẩn đoán nếu transport còn hoạt động.

## 9. RPC và client

Protocol riêng, có operation/version/requestId, frame length, checksum cho dữ liệu snapshot/log và giới hạn trước allocation. Không tái sử dụng Produce/Fetch data API với ý nghĩa consensus khác. Có các operation Vote, BeginQuorumEpoch, EndQuorumEpoch, QuorumFetch, FetchSnapshot, DescribeQuorum, CreateTopic, ReadMetadata và ReadLocalMetadata. Spec wire chi tiết và mã số được ghi trong implementation plan/protocol document trước coding.

Response mang node/leader hint và epoch nếu đã biết; hint không chứng minh leadership. Mã lỗi phân biệt NOT_LEADER, STALE_EPOCH, CLUSTER_MISMATCH, INCONSISTENT_VOTER_SET, UNSUPPORTED_VERSION, INVALID_REQUEST, OVERLOADED, REQUEST_TIMED_OUT, STORAGE_ERROR và NODE_UNAVAILABLE, bên cạnh lỗi topic. Không đổi số mã Phase 2.

Admin client bootstrap từ danh sách controller, correlate request, cập nhật hint và retry đọc/idempotent CreateTopic trong deadline chung. Retry CreateTopic giữ nguyên tên/cấu hình, không hứa exactly-once ở mức transport. Hết deadline phải giữ thông tin UNKNOWN nếu đã có lần gửi có thể được nhận; một lỗi NOT_LEADER mới không xóa sự không chắc chắn của lần trước.

Peer connection reconnect có backoff; tài nguyên request được giải phóng đúng một lần khi complete/timeout/disconnect. Response đến muộn không hoàn thành request khác. Lệnh format là thao tác local CLI, không phải RPC từ xa.

## 10. Snapshot và thu gọn log

Snapshot chứa toàn bộ committed metadata image tại applied boundary S, cluster identity, format/metadata version, fixed voter set, S và epoch của record ngay trước S, cùng checksum/length. Epoch/vote hiện tại nằm trong QuorumStateStore, không được phục hồi bằng cách ghi đè từ snapshot cũ. Snapshot không chứa pending commands hoặc waiter.

Chụp image tại event-loop boundary, serialize ra file tạm, flush rồi công bố immutable snapshot qua manifest bền vững. Không chỉ dựa vào rename mà giả định directory entry đã bền vững: implementation phải có protocol publish/checkpoint phù hợp filesystem hỗ trợ và fault tests. Startup chỉ nhận generation được công bố hoàn chỉnh.

Trigger theo lượng log kể từ snapshot trước; chỉ một snapshot creation đang chạy. ReadBarrier/LeaderChange không thay đổi catalog nhưng vẫn tính vào snapshot boundary và epoch. Giữ hai snapshot hoàn chỉnh mới nhất. Khi thu gọn, chỉ xóa segment có end <= boundary snapshot cũ hơn trong hai bản giữ lại; nhờ vậy snapshot cũ cùng suffix còn lại vẫn là đường recovery hoàn chỉnh. Không xóa file đang được đọc/truyền; dùng pin/reference lifetime có giới hạn.

Follower cần snapshot yêu cầu từng chunk bằng immutable snapshot ID và byte position. Leader không trộn chunk của hai snapshot; nếu ID không còn dùng được, follower bắt đầu lại từ snapshot mới. File tạm có tổng size giới hạn, kiểm tra chunk position, tổng length/checksum và identity/version trước cài đặt.

Cài đặt snapshot chỉ khi nó đưa node tiến lên và không hạ committed/applied boundary. Snapshot không được dùng để bỏ qua mâu thuẫn committed prefix đã phát hiện. Để đơn giản, node tuần tự hóa I/O, tạo generation mới gồm snapshot và empty log bắt đầu tại S, công bố atomically qua manifest rồi fetch suffix từ S; không giữ suffix cũ chưa được chứng minh khớp. Epoch/vote vẫn được bảo toàn. Crash tại mỗi bước phải chọn được generation cũ hoàn chỉnh hoặc mới hoàn chỉnh, không trộn hai generation.

Lỗi checksum của transfer hủy bản tạm và retry có backoff; lỗi storage cục bộ khi ghi/cài đặt khiến node failed. File tạm chưa công bố không dùng khi restart. Không tự fallback qua corruption của dữ liệu đã công bố; chỉ bỏ qua generation chưa hoàn tất theo manifest protocol. Snapshot cũ được giữ phục vụ đường recovery hợp lệ, không phải lý do âm thầm bỏ qua corruption.

## 11. Recovery, failure và shutdown

Startup: khóa directory, kiểm tra identity/version, phục hồi epoch/vote và active generation, kiểm tra snapshot/log nghiêm ngặt, dựng epoch lookup, khôi phục committed catalog tới checkpoint hợp lệ rồi mới tham gia quorum. Không đưa speculative suffix vào local catalog. Checkpoint commit không được vượt durable log/snapshot coverage; vượt là lỗi, không clamp để che mất dữ liệu.

Giữ chính sách Phase 1 đối với partial tail: chỉ cắt batch cuối thiếu byte khi cấu trúc hợp lệ và không ảnh hưởng committed boundary đã biết. Batch đủ byte checksum sai, header sai hoặc segment cũ hỏng khiến recovery thất bại. Uncommitted entry đầy đủ có thể được giữ; consensus quyết định commit hoặc truncate sau.

Lỗi append/flush, persist epoch/vote, commit checkpoint hoặc snapshot publication làm node ngừng tham gia quorum. Không tiếp tục vote hoặc ACK sau lỗi durability. Có thể giữ đường status chẩn đoán độc lập; không báo node ready.

Mất toàn bộ storage không được tự format rồi dùng lại voter ID cũ. Phase này hỗ trợ restart/catch-up với storage hợp lệ; quy trình thay thế mất đĩa cần thiết kế membership/recovery ở phase sau.

Shutdown ngừng admission, báo stepping down nếu có thể, kết thúc waiter, drain I/O trong deadline rồi đóng transport/storage và nhả lock. Hết deadline không báo flush thành công; không đóng file/nhả lock khi worker còn sửa storage. Hard process termination để lại recovery path như crash.

## 12. Giới hạn và cấu hình ban đầu

Các giá trị sau là baseline của bản spec để review, có thể tinh chỉnh khi benchmark; kiểm tra quan hệ giữa giới hạn khi startup.

| Cấu hình | Giá trị ban đầu |
|---|---|
| Voters/quorum | 3 / 2 |
| Fetch idle wait / RPC timeout | 100 ms / 1 s |
| Election timeout ngẫu nhiên | 1.5–3 s |
| Leader mất liên lạc đa số | 3 s |
| Admin deadline / shutdown deadline | 30 s / 30 s |
| Metadata batch / RPC frame tối đa | 1 MiB / 8 MiB |
| Fetch response data budget | 4 MiB, luôn đủ một batch hợp lệ |
| Snapshot chunk / snapshot file tối đa | 256 KiB / 64 MiB |
| Snapshot trigger | 16 MiB log mới kể từ snapshot trước |
| Pending admin requests | 1,024 mỗi node |
| Event queue / disk task queue | 4,096 / 256 |
| Tổng inbound/outbound bytes đang giữ | 64 MiB mỗi hướng, mỗi node |
| Snapshot transfer | Một download và tối đa hai upload mỗi node |
| Topics / tổng partitions | 128 / 1,024, kế thừa Phase 2 |

Queues dành riêng capacity cho vote/epoch/Fetch và disk completion; admin/snapshot không được chiếm hết. Không drop disk completion khi event queue đầy: capacity phải được reserve trước khi dispatch I/O. Tính cả queued và active buffers, decoded allocation và outbound writes trong budget. Overload phải trả lỗi hoặc đóng connection có kiểm soát, không tăng queue vô hạn.

Metadata append có thể batch nhiều record/request nhưng luôn flush trước khi tính durable progress. Không cần một fsync cho từng record. Tối ưu batching không được thay đổi contract commit. Giới hạn snapshot 64 MiB phải chứa được metadata image lớn nhất theo các giới hạn catalog; startup/config validation từ chối cấu hình không bảo đảm điều này.

## 13. CLI và quan sát

ControllerMain nhận config và data directory. CLI có generate-cluster-id, format, create-topic, metadata (linearizable mặc định), local-metadata và describe-quorum. Ví dụ chạy ba process trên ba port với một cluster ID, restart một node và xác minh UUID/topic không đổi.

Status/log có nodeId, role, epoch, leaderId, append/durable/commit/apply offsets, snapshot boundary, replica progress/lag, pending requests, queue/budget usage và lỗi storage cuối. Log role transition và lý do fail/step-down. Không cần metrics backend bên ngoài trong Phase 3.

## 14. Kiểm chứng và điều kiện hoàn thành

Test harness inject monotonic clock, RNG seed, network delivery/drop/duplicate/reorder và storage completions/failures. Simulation không thay thế test persistence/process thật. Invariants được kiểm tra sau mỗi event: một vote/epoch, committed prefix không đổi, apply chỉ committed, replica matching đúng, queue bounded, stale completion không gây ACK/commit sai.

| Nhóm | Kịch bản bắt buộc |
|---|---|
| Election | Candidate đồng thời; crash sau persist vote; epoch cao hơn; stale message; vote chống log kém cập nhật |
| Commit | Majority append chưa flush không commit; local flush chậm; current-epoch marker; pending prefix từ leader trước |
| Partition mạng | Cô lập leader; phía 2 node tiến triển; phía 1 node không commit; heal và hội tụ |
| Reconciliation | Đuôi khác epoch, cùng offset; truncate boundary; phát hiện committed conflict; Fetch cũ sau truncate |
| CreateTopic | Concurrent duplicate/conflict; mất response; retry qua leader mới; capacity gồm pending |
| Read | Barrier sau invocation; request đến sau không dùng barrier cũ; isolated leader; local view được gắn nhãn |
| Restart | Một node, toàn quorum; commit checkpoint chậm; append/flush/vote crash points; log corruption |
| Snapshot | Tạo/apply đồng thời; chunk trùng/trễ; checksum sai; leader đổi; crash từng bước publish/install/delete; restart từ nonzero offset |
| Catch-up | Follower offline vượt retention, nhận snapshot rồi suffix; bảo toàn topic UUID |
| Resource/lifecycle | Queue đầy, frame oversized, partial frame, snapshot chậm, shutdown khi disk task đang chạy |

Integration dùng ba JVM và TCP thật, chạy CreateTopic, failover, restart toàn cụm, process kill và snapshot catch-up. Network fault proxy hoặc transport harness tạo các partition tái lập được. Process kill không được ghi nhận là test mất điện; giả lập mất unflushed bytes là test riêng của storage fault model.

Hoàn thành khi mọi nhóm trên có bằng chứng chạy, metadata đã báo thành công không mất trong mô hình lỗi, minority không tiến commit, local catalogs hội tụ sau catch-up, và demo/CLI chạy từ hướng dẫn repository. Tests Phase 1/2 tiếp tục pass; không thay hành vi broker standalone.

## 15. Bước tiếp theo và tham chiếu

Sau review spec, implementation plan sẽ chốt API/file map, wire layouts, persistence manifest/checkpoint layout và crash ordering, cách storage primitive đáp ứng snapshot generation, test fixtures và task checkpoints. Đây là chi tiết triển khai của các contract trên, không được nới lỏng safety để thuận code.

- [Roadmap](2026-09-24-broker-roadmap.md)
- [Phase 1 storage](2026-09-24-storage-phase-1-design.md)
- [Phase 2 broker/client](2026-09-25-broker-phase-2-design.md)
- [KIP-595 — Metadata quorum](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158870126): tham chiếu election, pull-based replication và durability metadata; spec này dùng protocol riêng.
- [KIP-630 — Kafka Raft Snapshot](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158878566): tham chiếu snapshot trong metadata quorum; không yêu cầu tương thích định dạng Kafka.

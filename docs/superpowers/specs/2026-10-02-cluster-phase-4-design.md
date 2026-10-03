# Phase 4 — Cluster và quản lý partition

Ngày: 2026-10-02

Cập nhật: 2026-10-03. Người dùng đồng ý chuyển sang tiếp tục phục vụ khi chỉ mất liên lạc controller, sau review thiết kế. Bản này thay thế quyết định tự dừng theo heartbeat ngày 2026-10-02; đã cập nhật record granularity, epoch, version và tiêu chí kiểm chứng. Bản tổng hợp sửa đổi chờ review trước implementation plan; chưa triển khai Phase 4.

## 1. Mục tiêu và các quyết định

Kết nối broker với metadata quorum ba controller đã có ở Phase 3, tạo cluster nhiều broker với replication factor bằng một. Mục tiêu dài hạn là phát triển theo nguyên lý Kafka KRaft, dùng protocol và Java client riêng.

- Chuyển hoàn toàn sang cluster mode; không duy trì broker standalone như một chế độ runtime.
- Dùng cluster và thư mục dữ liệu mới; không migration dữ liệu Phase 2. Không cam kết mở trực tiếp metadata storage Phase 3 bằng format mở rộng của Phase 4.
- Controller quorum là nguồn metadata có thẩm quyền. Broker đăng ký, heartbeat, chịu fencing và chỉ áp dụng metadata đã commit.
- Broker đã RUNNING tiếp tục Produce/Fetch trên partition đang sở hữu khi chỉ mất liên lạc controller hoặc mất quorum. Startup/restart và thay đổi metadata vẫn cần quorum; committed fencing hoặc session rejection có thẩm quyền làm broker dừng phục vụ theo mục 6. Không có serving lease hoặc chế độ tùy chọn tự dừng theo heartbeat.
- Controller tự phân bổ partition khi tạo topic; chưa có assignment thủ công, di chuyển partition hoặc tự cân bằng lại.
- Client bootstrap qua broker. CreateTopic được broker chuyển tới controller leader; Produce/Fetch đi thẳng tới broker giữ partition.
- Broker đồng bộ metadata theo log và snapshot với vai trò observer, không bỏ phiếu hoặc tham gia đa số commit.
- Giữ APPENDED/FLUSHED. Fetch công bố highWatermark là exclusive read boundary, trong Phase 4 RF=1 bằng logEndOffset; không hàm ý đã flush. Replication dữ liệu, quản lý ISR, tính high watermark từ replica progress và tự failover thuộc Phase 5.

Broker chứa partition ngừng hoạt động thì partition đó unavailable. Mất vĩnh viễn bản sao dữ liệu duy nhất không thể được chữa bằng metadata quorum.

## 2. Ranh giới kiến trúc và ownership

| Thành phần | Trách nhiệm |
|---|---|
| Controller metadata service | Validate lệnh, quản lý broker sessions, timeout, pending operations và quyết định assignment |
| Metadata state machine/image | Apply committed records theo thứ tự, cung cấp image bất biến gồm brokers, topics, assignments |
| Broker lifecycle | Identity, registration, heartbeat, admission gate và shutdown |
| Broker metadata observer | Discover leader, fetch committed log, tải/cài snapshot, phục hồi metadata cache |
| Partition manager | Đối chiếu assignment với local inventory, mở log và công bố trạng thái phục vụ |
| Partition runtime | Append, flush, fetch và tuần tự hóa thao tác partition; không quyết định ownership |
| ClusterClient | Bootstrap, metadata cache, routing, connection pool có giới hạn và retry |
| BrokerClient | Một kết nối tới một broker, correlation, deadline và phân loại NOT_SENT/UNKNOWN |

Controller giữ mô hình event loop và ordered disk I/O của Phase 3. Broker có lifecycle/metadata control loop tuần tự; I/O mở log và snapshot chạy ngoài network/control loop. Mỗi completion mang session và generation để kết quả cũ không cấp lại quyền phục vụ. Tài nguyên đã cấp vẫn phải được thu hồi đúng một lần.

MetadataService hiện đang sở hữu local metadata log được thay bằng ranh giới đọc metadata image và chuyển tiếp lệnh. Không chạy song song hai nguồn có quyền quyết định metadata. Tái sử dụng storage, partition runtime, protocol envelope và transport qua contract hiện có; không để kiểu Netty lan vào logic cluster.

## 3. Mô hình lỗi và giới hạn bảo đảm

Mạng có thể mất kết nối, chậm, lặp hoặc đổi thứ tự phản hồi. Process có thể crash, pause rồi chạy tiếp. Storage tuân theo contract flush của Phase 1/3. Node không Byzantine; clusterId là kiểm tra danh tính cấu hình, không thay thế authentication.

Metadata đã báo thành công tồn tại qua restart trong mô hình lỗi của quorum. Minority không commit registration, assignment hoặc thay đổi fencing mới. Broker đã RUNNING có thể tiếp tục đọc/ghi các log khỏe theo committed image cuối cùng dù không nhận heartbeat response trong thời gian dài. Không bảo đảm mọi request thành công: storage/network faults, fencing đã biết và routing cache mới hơn vẫn có thể khiến partition unavailable. Broker startup/restart không được tự phục vụ chỉ vì cache cũ ghi unfenced.

Fencing ở controller và dừng admission tại broker không xảy ra đồng thời. Controller timeout phát hiện broker không còn liên lạc để đề xuất committed fencing; broker bị cô lập có thể chưa biết quyết định đó và vẫn phục vụ client có routing cũ. Đây là giới hạn được chấp nhận, không tuyên bố broker đã dừng chỉ vì controller đã fence. Phase 4 giữ assignment cố định, không cấp partition sang bản sao khác. Không cho phép clone data root hoặc chạy cùng identity trên storage độc lập. Phase 5 phải bổ sung fencing tại giao thức replication trước khi hỗ trợ chuyển leader dữ liệu.

Broker kiểm tra session/assignment, fencing đã biết và trạng thái log tại admission và trước khi bắt đầu thao tác partition. Không có deadline heartbeat để cấp hoặc thu hồi quyền data-plane. Timeout RPC, controller session tracking và backoff vẫn dùng clock monotonic.

## 4. Identity, format và tương thích

Broker format nhận clusterId, brokerId và data root mới/rỗng; sinh storageId bền vững để phân biệt lần format. Manifest chứa role, format version và checksum. Startup đối chiếu cấu hình, khóa độc quyền data root và từ chối thư mục standalone, thiếu identity, sai cluster/broker hoặc format không hỗ trợ. Không tự format hoặc ghi đè khi startup.

Controller lưu storageId cùng registration. Một brokerId đã đăng ký không được tái sử dụng với storageId khác trong Phase 4, kể cả sau fencing. Mất toàn bộ disk cần quy trình thay thế riêng ở phase sau; không format disk mới rồi giả làm broker cũ.

Mỗi process startup tạo incarnationId mới. Retry registration trong cùng process giữ nguyên incarnationId và nội dung request. Controller cấp brokerEpoch tăng đơn điệu qua committed metadata; epoch mới không được dùng trước commit/apply. Cùng incarnation đã đăng ký trả lại cùng kết quả; retry cũ sau khi bị thay thế nhận lỗi stale, không thay thế phiên hiện tại.

Registration mới mang expectedBrokerEpoch lấy từ metadata controller; lần đăng ký đầu dùng giá trị chưa tồn tại. Điều kiện này được kiểm tra cùng incarnation và committed/pending session. Retry giữ nguyên expected epoch; request cũ không được tự sửa expected epoch rồi thay thế phiên mới. Cùng incarnation hiện tại vẫn trả lại kết quả idempotent. Cơ chế compare-and-set này ngăn registration trễ tái chiếm brokerId mà không phải lưu lịch sử incarnation vô hạn.

Metadata record/image mở rộng phải có version mới, không diễn giải lại bytes v1. Tách storage format version, RPC/schema version và cluster metadata.version. Cluster Phase 4 được format mới và chạy cùng phiên bản; không yêu cầu rolling upgrade hoặc import dữ liệu cũ. Codec/vector tests v1 vẫn bảo vệ contract lịch sử. Implementation plan phải chỉ rõ phiên bản nào được nhận tại từng runtime endpoint; phiên bản không hỗ trợ trả lỗi tường minh.

Format controller bootstrap một FeatureLevelRecord cho metadata.version với cùng mức được cấu hình trên ba voter; record này thuộc committed history trước admission broker/topic và luôn có trong snapshot. Bootstrap phải idempotent qua election/restart. Broker khai báo min/max metadata.version trong registration; controller từ chối nếu mức cluster nằm ngoài dải hỗ trợ, broker cũng kiểm tra khi đọc log/snapshot. Controller binary phải hỗ trợ mức cluster trước khi tham gia quorum. Không cho thay đổi metadata.version động hoặc downgrade trong Phase 4. Phase 5 phải thiết kế đường nâng cấp từ dữ liệu Phase 4 và compatibility tests; thêm version field không tự tạo ra migration hay cho phép reset dữ liệu mặc định.

## 5. Vòng đời broker

Luồng chính: STARTING → RECOVERING → FENCED → RUNNING. Lỗi fatal đưa về FAILED; shutdown đưa về STOPPING. Mất heartbeat response khi RUNNING chỉ cập nhật trạng thái kết nối/diagnostics và retry, không chuyển lifecycle sang FENCED.

1. STARTING: khóa directory, đọc và validate identity/config.
2. RECOVERING: phục hồi metadata generation và local partition inventory; chưa phục vụ Produce/Fetch.
3. FENCED: đăng ký phiên, đồng bộ metadata, reconcile partition; heartbeat vẫn hoạt động để báo tiến độ.
4. RUNNING: chỉ sau khi controller cho phép phiên hiện tại bằng committed metadata, broker đã apply quyết định đó cùng requiredMetadataOffset và recovery local đã hoàn tất. Heartbeat thành công đơn lẻ không mở admission.

Incarnation khác dùng cùng brokerId bị từ chối khi registration cũ còn unfenced hoặc fencing chưa commit. Sau khi phiên cũ fenced, registration mới cùng storageId có thể được commit. Registration mới luôn bắt đầu fenced; không kế thừa quyền hoạt động của phiên trước.

Để tránh vòng chờ readiness/assignment, broker không có partition có thể hoàn tất catch-up rồi được unfence. Partition lỗi riêng lẻ không ngăn broker phục vụ những partition khỏe; broker phải báo trạng thái lỗi, không báo mọi partition ready.

Khi biết mình bị fence bằng committed metadata hoặc session rejection có thẩm quyền theo mục 6, broker dừng admission Produce/Fetch, đánh thức Fetch đang long-poll và kiểm tra lại gate của tác vụ còn trong hàng đợi. Mất liên lạc controller đơn thuần không hủy các request này. Ghi đã bắt đầu có thể hoàn tất; không rollback log để giả vờ request chưa xảy ra. ACK thành công, nếu được trả, vẫn phải đáp ứng APPENDED/FLUSHED tương ứng. Lỗi sau thời điểm có thể append phải giữ outcome UNKNOWN. Không trả mã retry-safe cho thao tác có thể đã ghi.

Shutdown ngừng admission, kết thúc waiters, dừng heartbeat/observer có thứ tự, drain workers trong deadline và đóng tài nguyên. Không nhả lock/đóng file khi I/O còn sửa storage. Không thêm graceful reassignment trong phase này.

## 6. Heartbeat, fencing và đổi controller leader

Heartbeat mang clusterId, brokerId, storageId, incarnationId, brokerEpoch, sequence, appliedOffset và trạng thái broker. Chỉ có một heartbeat đang chờ trên mỗi session. Callback phải khớp request, session và controller generation; stale response không làm lùi state hoặc mở lại admission. Phase 4 chưa có wantShutdown; controlled shutdown với di chuyển leader thuộc Phase 5 và được thêm bằng version protocol có định nghĩa đầy đủ.

Heartbeat thông thường cập nhật liveness trong memory và trả committed session status; không append ReadBarrier hay metadata record mỗi lượt. Response không là lease hoặc chứng minh linearizable cho request. Registration/fence/unfence/assignment vẫn phải commit/apply trước khi báo thay đổi thành công. Nếu heartbeat dẫn tới một thay đổi lifecycle, phần kết quả thay đổi phải chờ commit; timeout không chứng minh thay đổi chưa xảy ra. ReadBarrier hiện có tiếp tục phục vụ API đọc linearizable của Phase 3.

Giữ check-quorum của Phase 3: controller leader tự bước xuống khi không có liên lạc hợp lệ từ đa số voter trong timeout. Observer/broker heartbeat không tính vào đa số đó. Check-quorum có cửa sổ phát hiện trễ, không thay thế commit hoặc chứng minh controller là leader tại mọi thời điểm. Controller cũ có thể trả trạng thái cũ; trạng thái đó không được cấp registration/ownership mới hay mở lại broker đã bị chặn.

Controller theo dõi liveness bằng thời điểm nhận request mới hợp lệ của session hiện tại, không dùng timestamp do broker gửi. Sequence cũ/lặp, sai epoch và phản hồi lặp không kéo dài phiên. Leader mới reset tracking tạm thời; tại mỗi broker chỉ ghi nhận sequence tăng trong lượt theo dõi hiện tại. Timeout làm controller enqueue fencing; authoritative image chỉ thay đổi sau commit/apply. Pending fence/unfence/registration cho cùng broker được tuần tự hóa; heartbeat mới không âm thầm hủy fencing đã append.

Broker phân biệt các kết quả:

- Timeout, disconnect, NOT_CONTROLLER và OVERLOADED: giữ quyền phục vụ đã có, tìm leader/retry có backoff; không suy ra fencing.
- Metadata đã commit cho biết phiên hiện tại fenced hoặc đã bị thay thế: đóng admission.
- Response session rejection tường minh (fenced/stale broker epoch/registration không còn hợp lệ) chỉ được xử lý khi khớp request/session và mang committed metadata offset cùng controller epoch không lùi so với trạng thái đã biết. Không lấy một leader hint hoặc lỗi routing chung làm quyết định thu hồi quyền. Broker đóng admission và bắt kịp metadata để đối chiếu trước khi phục vụ lại.
- Response báo unfenced thông thường không tự mở gate. Sau một rejection, phải bắt kịp ít nhất rejection offset và hoàn tất chu trình recovery/unfence mới; chỉ một committed quyết định cho phép có offset mới hơn mốc chặn mới mở gate. Controller phải ghi một xác nhận unfence mới trong chu trình này kể cả image hiện tại đã unfenced, để heartbeat cũ không tái sử dụng quyết định trước đó.

Response state mang metadata offset cho phép broker bỏ qua fenced response cũ đến sau quyết định unfence mới đã apply. Generation/correlation checks được áp dụng trước xử lý response. Không biến mọi heartbeat error thành lỗi fatal. Storage/identity/version không hợp lệ vẫn fail closed như các mục liên quan.

Leader mới commit/apply LeaderChange trước xử lý lifecycle. Không phục hồi timestamp heartbeat monotonic của leader cũ. Bắt đầu cửa sổ quan sát mới có giới hạn từ khi leader sẵn sàng; không có heartbeat hợp lệ trong cửa sổ đó thì đề xuất fencing. Trước heartbeat mới hợp lệ tại leader hiện tại, broker không đủ điều kiện nhận assignment mới. Leader mới vẫn từ chối incarnation thay thế khi image còn ghi phiên cũ unfenced.

Mỗi chu trình đăng ký/unfence chụp requiredMetadataOffset từ committed image sau registration hoặc sau mốc chặn cần phục hồi. Broker phải apply tới ít nhất mốc đó và báo recovery hoàn tất; controller commit quyết định cho đúng session. Broker chỉ chạy khi đã apply quyết định mới này. Target được chụp cố định cho chu trình, không đuổi theo metadata đang tăng; vẫn cần target này dù không còn heartbeat barriers. Chỉ reconnect mà không có fencing/rejection không bắt broker RUNNING đi lại chu trình unfence.

## 7. Metadata model và tạo topic

Image chứa cluster identity/metadata.version, appliedOffset, broker registrations và trạng thái fencing, topics theo UUID/tên, cùng assignment cho mỗi partition. TopicRecord chứa UUID/tên/số partition; PartitionRecord chứa topicId, partitionId, replicas một phần tử, designated leader, leaderEpoch và partitionEpoch. Fencing làm partition unavailable trong image thông qua trạng thái broker; không xóa assignment và không chứng minh broker cô lập đã ngừng phục vụ.

leaderEpoch tăng khi một quyền leader mới của partition được công bố, kể cả cùng brokerId nhưng phiên mới; không tăng chỉ vì retry hoặc controller đổi leader. partitionEpoch tăng cho mỗi thay đổi persisted partition state; thay leader cũng tăng partitionEpoch. Hai epoch khởi tạo bằng 0. BrokerEpoch thuộc registration, không dùng thay hai epoch này. Khi session mới được unfence, controller công bố cập nhật quyền leader/epoch của các partition được giao cùng quyết định unfence trong một batch nguyên tử có giới hạn. Nếu batch vượt budget, từ chối trước append và giữ fenced; không công bố quyền dở dang. Broker-level fencing không sửa PartitionRecord thì không tăng partitionEpoch. Phase 5 dùng expected partitionEpoch để compare-and-set cập nhật partition và thêm PartitionChangeRecord dạng delta.

Chưa thêm ISR chỉ để giữ chỗ: Phase 5 sẽ định nghĩa membership/transition lúc offline/fenced/rejoin và mở rộng schema có version. Không mặc định ISR = replicas trong mọi trạng thái. Không triển khai quản lý ISR ở Phase 4.

CreateTopic đi client → broker → controller leader. Controller kiểm tra tên, số partition, RF=1 và capacity gồm committed/pending. Controller chọn broker unfenced có heartbeat mới hợp lệ nhận trong leader epoch hiện tại, chưa quá session timeout, không có pending fencing và còn capacity.

Phân bổ lần lượt từng partition: chọn broker có tổng assignment committed cộng pending nhỏ nhất; hòa thì brokerId nhỏ hơn. Sau mỗi lựa chọn tăng bộ đếm dự kiến trước khi phân bổ partition tiếp theo. Broker mới chỉ nhận assignment từ lệnh tạo topic về sau. Không có broker đủ điều kiện thì trả lỗi retryable và không tạo topic dở dang.

Một lệnh CreateTopic tạo đúng một batch nguyên tử chứa một TopicRecord và N PartitionRecord theo thứ tự partition ID. Chốt rõ đơn vị nguyên tử là batch gồm nhiều record, không phải một record lớn chứa cả topic. Validate toàn bộ trước append; nếu vượt batch/frame limit thì từ chối, không chia sang nhiều batch. Apply xây image mới rồi công bố một lần tại batch end; snapshot/replay không nhìn thấy topic thiếu assignment. Phase 4 chưa cần metadata transactions nhiều batch.

- Cùng tên/cấu hình đã tồn tại: trả cùng UUID và assignment đã commit, không chạy thuật toán gán lại.
- Cùng tên/cấu hình đang pending: dùng chung thao tác; cấu hình khác trả conflict.
- Nếu broker bị fence sau khi assignment đã commit, topic vẫn tồn tại và partition chờ broker quay lại.
- Thành công nghĩa là metadata đã commit/apply ở controller, chưa nghĩa là mọi log đã mở.
- Timeout/disconnect sau admission có thể UNKNOWN. Broker chuyển tiếp phải giữ trạng thái không chắc chắn qua retry và đổi leader.

Controller admin client hiện có tiếp tục dùng cùng command validation; không có đường CreateTopic bỏ qua assignment. Ứng dụng bình thường chỉ cần bootstrap broker.

## 8. Đồng bộ metadata bằng observer

Broker là observer không có vote, không ảnh hưởng quorum size, durable majority hoặc eligibility controller leader. Đường observer fetch được phân biệt với voter replication; broker ID không được vượt qua validation voter chỉ để tái sử dụng RPC.

Baseline Phase 4 chỉ truyền metadata batches đã commit cho broker. Observer gửi nextOffset và prefix epoch; controller trả các batch liên tiếp không vượt committed boundary, commit offset và leader hint. Control records vẫn chiếm offset dù không thay image. Hint không phải bằng chứng leadership. Mỗi observer chỉ có một fetch đang chờ; phản hồi được ràng buộc request/session/generation.

Offset là exclusive và nằm trên batch boundary. Trong generation hợp lệ:

`snapshotEndOffset <= appliedOffset <= durableReceivedOffset <= receivedOffset <= verifiedCommittedOffset`

Metadata được ghi/flush local trước apply; checkpoint không vượt coverage bền vững. Snapshot có thể đi trước log cũ khi được cài bằng generation mới. Cache cục bộ chỉ là bản sao của quorum, không là nguồn tạo metadata.

Broker không bắt buộc tải snapshot mỗi lần startup. Phục hồi image/checkpoint hợp lệ rồi tiếp tục fetch. Nếu prefix cần đọc đã bị thu gọn, tải snapshot và đọc suffix từ snapshotEndOffset. Nếu prefix đã apply xung đột với committed history, broker failed; không chữa bằng cách xóa trạng thái và tiếp tục phục vụ.

Apply theo thứ tự trên control loop rồi công bố immutable image. Heartbeat báo appliedOffset, không báo receivedOffset như thể đã áp dụng. Metadata broker trả cho client là local committed view, có thể cũ; trả clusterId và appliedOffset, không tự gắn nhãn linearizable. Broker có thể chưa thấy topic vừa tạo ở broker khác; client refresh/retry trong deadline.

## 9. Snapshot và recovery

Snapshot chứa brokers, session epochs, storage identities, fencing, topics, assignment, partition epochs, version, cluster identity và offset/epoch boundary. Không chứa heartbeat timestamps/sequence tracking tạm thời, pending requests hoặc trạng thái log local ready. Snapshot có metadata.version và cả leaderEpoch/partitionEpoch; không tự cấp quyền cho process mới.

Transfer dùng immutable snapshot ID, chunk position, tổng size và checksum có giới hạn. Tải vào vùng tạm; validate toàn bộ trước publication. Cài snapshot tuần tự với metadata I/O, tạo log generation mới ở boundary, flush files/directory và công bố manifest bền vững. Crash chọn được generation cũ hoặc mới hoàn chỉnh; không trộn image cũ với suffix mới. Startup không dùng file tạm chưa được công bố.

Tái sử dụng SnapshotStore, SnapshotTransfer và durable publication của Phase 3 khi contract phù hợp, nhưng observer không sở hữu hard state vote. Metadata image mới lớn hơn v1: phải sửa các bound/size assumptions đang dựa vào 128 topic descriptors và một disk slice 256 KiB. Tính cả broker descriptors và từng partition assignment trong validation và budgeting.

Checksum lỗi khi transfer: hủy download và retry có backoff. Corruption local đã publication hoặc lỗi flush: broker failed và đóng data admission; không âm thầm fallback qua dữ liệu hỏng. Snapshot tải được không tự cấp quyền phục vụ.

## 10. Partition provisioning và quyền phục vụ

Partition manager reconcile immutable image với local inventory. Metadata apply không chờ mở tất cả log. Trạng thái mỗi partition gồm chưa provision, đang mở, ready hoặc failed; trạng thái local này không phải assignment authority.

Local inventory bền vững nằm ngoài từng thư mục partition, gắn topicId/partition với storageId và trạng thái provisioning. Quy trình khởi tạo ghi intent trước, tạo/flush cấu trúc log rồi ghi completion bền vững trước khi công bố ready. Crash ở bước intent chỉ được tiếp tục khởi tạo khi chứng minh partition chưa từng được cho phép nhận dữ liệu. Đã có completion mà thiếu log thì unavailable, không tạo lại rỗng. Inventory thiếu/hỏng không được hiểu là mọi partition đều mới.

Disk mới có storageId khác bị registration từ chối như mục 4. Giới hạn này cùng inventory ngăn mất partition directory hoặc mất toàn bộ disk bị che bằng log rỗng. Local directory chưa có trong image hiện tại không được tự xem là đã xóa hoặc tự dọn; có thể broker chưa catch-up. Giữ directory không phục vụ và báo chẩn đoán. DeleteTopic/tombstone và cleanup có thẩm quyền thuộc mốc lifecycle sau trong roadmap; không thêm trạng thái deleted không có nguồn quyết định.

Produce/Fetch chỉ được bắt đầu khi lifecycle RUNNING và không có fencing/session rejection chưa được giải quyết, image cho phép broker và partition, request epoch hợp lệ, runtime đang ready. Không thêm điều kiện heartbeat freshness vào data admission. Tác vụ queued kiểm tra lại trước I/O; completion mở log cũ chỉ được công bố nếu vẫn khớp assignment/session/generation. Một partition mở thất bại không dừng các partition khỏe; lỗi metadata hoặc identity không đáng tin cậy dừng toàn broker.

Sau restart cùng dữ liệu, APPENDED không hứa sống sót trước mất điện; FLUSHED theo contract local storage. Không gán ý nghĩa replication vào hai mức ACK này. Fetch trả thêm highWatermark exclusive và chỉ trả batch trong read boundary đã chụp. Phase 4 highWatermark = logEndOffset tại cùng ảnh chụp runtime, nên vẫn thấy dữ liệu chưa flush. Giữ riêng logEndOffset; highWatermark không thay thế durableEndOffset hay bảo đảm FLUSHED. Phase 5 đổi cách tính highWatermark theo replica progress, vẫn giữ contract read boundary. Field mới nằm trong protocol version mới, có golden vector và test boundary.

## 11. Protocol, routing và retry client

Protocol giữ operation/version/requestId và giới hạn trước allocation. Thêm contract registration, heartbeat và observer fetch/snapshot; data protocol cần metadata routing và epoch fencing. Không đổi nghĩa/số mã lỗi cũ; version mới hoặc operation mới mang các field bổ sung. Wire layouts, enum numbers và golden vectors được chốt trong implementation plan trước coding.

Metadata trả clusterId, appliedOffset, broker IDs/advertised endpoints, topic UUID, partition replicas/leader, leader epoch và broker epoch liên quan. Một broker không thể khẳng định remote log đã mở chỉ từ assignment; readiness cuối cùng được kiểm tra tại broker đích. Produce/Fetch mang identity/epoch đích để server từ chối stale routing trước append.

ClusterClient pin clusterId từ cấu hình hoặc bootstrap hợp lệ đầu tiên; từ chối trộn metadata của cluster khác. Bootstrap dùng danh sách broker, thử lại với backoff trong deadline. Kết nối được quản lý theo broker ID, endpoint và generation, có giới hạn số lượng/buffer/in-flight và được đóng khi client đóng. Endpoint thay đổi không cho response cũ hoàn thành request mới.

Request nhiều partition được nhóm theo broker đích, gửi và ghép kết quả theo từng partition. Tổng budget, maxBytes của Fetch và deadline phải được phân bổ cho cả thao tác, không nhân lên vô hạn theo số broker. Lỗi một broker không che kết quả đã biết của broker khác. Producer giữ thứ tự batch theo partition trong quá trình refresh/retry; không để batch sau vượt batch trước đang chờ kết quả.

Refresh được gộp giữa các request cùng cần metadata. Lỗi sai leader, stale epoch, fenced hoặc chưa ready kích hoạt refresh/backoff có giới hạn. Không thay UUID topic cũ bằng topic cùng tên một cách âm thầm.

| Thao tác/kết quả | Retry |
|---|---|
| Metadata | Có thể retry trong deadline |
| Fetch | Có thể retry với cùng offset và các giới hạn còn lại; không tự nâng consumer offset |
| CreateTopic | Retry cùng tên/cấu hình, giữ UNKNOWN nếu từng có lần admission không rõ kết quả |
| Produce chắc chắn chưa gửi | Có thể retry trong deadline |
| Produce bị từ chối chắc chắn trước append | Có thể refresh rồi retry |
| Produce đã gửi, timeout/disconnect hoặc lỗi sau khi có thể append | Trả UNKNOWN, không tự gửi lại |

Chỉ retry phần partition đủ điều kiện, không gửi lại phần đã thành công. Hủy future không chứng minh thao tác không xảy ra. Broker forwarding áp dụng cùng deadline chung, không reset deadline khi đổi controller. Phase này không có producer idempotence hoặc exactly-once.

## 12. Cấu hình, tài nguyên và quan sát

Baseline để review và triển khai ban đầu:

| Cấu hình | Giá trị/contract |
|---|---|
| Controllers / quorum | 3 / 2, membership cố định |
| Broker count tối đa | 32; demo dùng 3 |
| RF | Chỉ nhận 1 |
| Topics / tổng partitions | 128 / 1.024; capacity có tính pending |
| Heartbeat interval / RPC deadline | 1 s / 2 s |
| Controller check-quorum timeout | Kế thừa cấu hình leaderContactTimeout Phase 3; broker/observer không gia hạn quorum |
| Controller session timeout | 10 s; leader mới dùng cửa sổ quan sát 10 s |
| Client operation / shutdown deadline | 30 s / 30 s |
| Observer fetch idle wait / data budget | 100 ms / 4 MiB |
| Metadata batch / internal frame tối đa | 1 MiB / 8 MiB |
| Snapshot chunk / envelope tối đa | 256 KiB / 64 MiB; validate bound của image mở rộng |
| Observer fetch / download mỗi broker | Tối đa 1 / 1 |
| Client broker connections | Tối đa 32; lazy connect |

Timeout chỉ phục vụ RPC, retry và failure detection tại controller, không là serving lease. Validation yêu cầu heartbeat interval < RPC deadline < controller session timeout, giá trị dương và budget đủ ít nhất một batch hợp lệ. Các giới hạn broker/topic/partition là cấu hình/validation có kiểm tra kích thước và tài nguyên, không encode thành độ dài mảng cố định hay semantic constant của format. Decoder vẫn phải áp dụng bound trước allocation; tăng cấu hình không được vượt hard byte/frame limits hoặc khả năng reader đang triển khai. Advertised host được giới hạn 255 UTF-8 bytes, port 1–65535; mỗi broker một endpoint trong phase này. Broker IDs duy nhất trong namespace broker; role phân biệt với voter IDs.

Kế thừa bounded queues/bytes của Phase 2/3, bổ sung accounting cho forwarding, metadata observers và client fan-out. Reserve capacity cho lifecycle và voter consensus, không để observer/snapshot/admin chiếm hết. Disk completion không được drop; snapshot upload pin có thời hạn và số lượng giới hạn toàn controller. Heartbeat liveness không làm metadata log tăng khi không có thay đổi trạng thái; API đọc linearizable vẫn dùng ReadBarrier riêng.

Status/log gồm clusterId, brokerId, incarnation/broker epoch, lifecycle và lý do fence, heartbeat age, connectivity và controller session status, controller leader/epoch, received/durable/applied offsets, metadata lag, snapshot progress, partition ready/failed, queue/budget và lỗi storage cuối. Không cần metrics backend ngoài trong Phase 4.

## 13. Kiểm chứng và điều kiện hoàn thành

Dùng clock/scheduler, network faults và storage harness có tính xác định. Kiểm tra invariants sau từng event: chỉ apply committed; topic/assignment nguyên tử; stale callback không mở gate; observer không ảnh hưởng majority; retry-safe error không xuất hiện sau possible append; inventory không cho tái tạo dữ liệu đã mất; mọi queue/buffer có bound.

| Nhóm | Kịch bản bắt buộc |
|---|---|
| Identity | Format rỗng, standalone bị từ chối, sai cluster/broker/version, duplicate lock, storageId khác |
| Registration | Retry cùng incarnation, incarnation mới bị từ chối khi phiên cũ active, fencing commit rồi restart, delayed registration cũ |
| Heartbeat | Drop/reorder/duplicate, stale epoch, pause/resume; timeout không fence local; session rejection đúng/sai generation; fenced response cũ sau unfence mới; steady heartbeat không append log |
| Leader change | LeaderChange trước admission, heartbeat timestamps không được kế thừa, catch-up/unfence qua leader mới |
| Assignment | Nhiều lệnh đồng thời tính pending, tie-break xác định, không eligible broker, broker mới không rebalance, oversized command |
| Atomic metadata | TopicRecord + N PartitionRecord công bố nguyên tử qua apply/replay/snapshot/crash; oversized batch bị từ chối |
| Epoch/version | leaderEpoch và partitionEpoch tăng đúng; metadata.version bootstrap qua election/restart; incompatible broker/controller bị từ chối; snapshot bảo toàn version |
| Fetch boundary | RF=1 highWatermark bằng LEO cùng snapshot; không đồng nghĩa flushed; response codec và giới hạn đọc |
| Provisioning | Crash từng bước intent/create/completion; mất directory sau ready; mở log completion cũ; một partition hỏng |
| Observer | Chỉ committed prefix, leader đổi, prefix epoch sai, offset gap/duplicate, snapshot khi log đã thu gọn |
| Snapshot | Checksum/version/identity sai, crash publication/install, metadata image lớn nhất, observer load không làm nghẽn voters |
| Routing | Bootstrap qua một broker nhưng đọc/ghi cả ba, stale cache, endpoint đổi, fan-out và global budgets |
| Retry | Produce UNKNOWN không gửi lại, definite rejection được retry, partial success không gửi lại, CreateTopic mất response |
| Restart/fault | Restart một broker/cả cluster; controller crash/minority/lost quorum: broker đã RUNNING còn đọc/ghi, broker mới/restart không tự ready; committed fencing tới broker thì dừng; healed cluster hội tụ |

Acceptance dùng ba controller JVM và ba broker JVM trên TCP thật, network fault proxy hoặc harness tái lập được, cùng Java client/CLI. Metadata và dữ liệu FLUSHED phục hồi theo contract khi restart với storage còn nguyên. Process kill không được gọi là bằng chứng power-loss; storage fault model kiểm tra mất unflushed bytes riêng.

Strict durability/process acceptance chạy trên Linux/WSL ext4 đã xác minh directory durability, không mặc định coi /mnt/d là ext4. Native Windows core/unit tests không chứng minh strict durability. Giữ test storage/protocol/consensus phù hợp; chuyển test integration phụ thuộc standalone sang cluster hoặc fixture có injected metadata, không giữ một runtime standalone chỉ để test cũ pass.

Chạy focused tests theo thay đổi, sau đó mvn clean verify trên JDK 21/Maven 3.9.x và fault/process campaigns của Phase 3/4. Lưu evidence và giới hạn nền tảng. Phase 4 hoàn thành khi tất cả nhóm bắt buộc có bằng chứng chạy và demo từ tài liệu tạo topic, route, restart và fencing đúng contract.

## 14. Tài liệu và bước triển khai tiếp theo

Implementation plan phải cụ thể hóa file/API map, versioned record và wire layouts, error/outcome mapping, schema snapshot, durable provisioning inventory, disk ordering và crash points, heartbeat/lifecycle scheduling không append log thường kỳ, resource budgets và fixtures. Những chi tiết đó phải giữ nguyên contract trong spec; nếu phát hiện cần đổi bảo đảm thì quay lại review thiết kế.

Cập nhật protocol, broker/controller configuration và operation docs, format/start scripts, Java example, verification guide và README khi triển khai. Tài liệu Phase 1–3 vẫn mô tả lịch sử của từng phase; tài liệu runtime Phase 4 phải nói rõ standalone đã bị thay thế. Bản spec này chưa thay đổi lệnh chạy của code hiện tại.

Trình tự triển khai đề xuất: metadata schema/controller lifecycle → observer và recovery → broker provisioning/admission → forwarding và cluster client → fault/process acceptance. Đây là thứ tự phụ thuộc, chưa phải implementation plan đã được duyệt.

Ngoài phạm vi: migration từ Phase 2/3, rolling upgrade và thay metadata.version động, broker replacement mất disk, unregister/delete topic, combined mode, controlled shutdown chuyển leader, reassignment/rebalance, data replication/failover, quản lý ISR và tính high watermark từ replication, consumer groups, security và producer idempotence.

## 15. Tham chiếu

- [Roadmap](2026-09-24-broker-roadmap.md)
- [Phase 2 broker/client](2026-09-25-broker-phase-2-design.md)
- [Phase 3 metadata quorum](2026-09-28-metadata-quorum-phase-3-design.md)
- [Controller storage contract](../../controller-storage-v1.md)
- [Controller verification](../../controller-verification.md)
- [KIP-595 — Metadata quorum](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158870126): broker observer, metadata log và snapshot catch-up.
- [KIP-630 — Kafka Raft Snapshot](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/158864763/KIP-630%2BKafka%2BRaft%2BSnapshot): snapshot boundary và tiếp tục đọc log. Dự án không cam kết tương thích Kafka protocol/binary format; timeout và lifecycle trong spec này là quyết định riêng của Phase 4.

## 16. Kết quả review ngày 2026-10-03

Tiếp thu: tách availability data plane khỏi heartbeat timeout; bỏ ReadBarrier thường kỳ cho heartbeat; TopicRecord/PartitionRecord trong một batch; tách leaderEpoch/partitionEpoch; metadata.version tối thiểu; highWatermark có contract RF=1; bổ sung roadmap về nâng cấp, unregister/thay disk và delete topic.

Giữ: quorum commit cho lifecycle mutation, fixed catch-up target, request/session/generation fencing, inventory intent/completion, UNKNOWN không tự retry Produce, bounded observers và snapshot publication. Check-quorum đã có ở Phase 3, không thay thế commit hoặc linearizable read barrier. Không khẳng định inventory mạnh hơn Kafka khi chưa có đối chiếu đầy đủ.

Hoãn: ISR khi chưa có state transitions, wantShutdown khi chưa có controlled shutdown, combined mode và metadata transactions nhiều batch. Không thêm hai chế độ serving lease để tăng ma trận hành vi. Unregister không phục hồi bản sao RF=1 đã mất; stray directory không phải bằng chứng DeleteTopic.

Tham chiếu đã đối chiếu khi review:

- [Kafka 4.1 BrokerLifecycleManager](https://raw.githubusercontent.com/apache/kafka/4.1/core/src/main/scala/kafka/server/BrokerLifecycleManager.scala): heartbeat timeout lên lịch retry, không tự dừng broker RUNNING theo serving timeout. Thiết kế dự án không tuyên bố sao chép toàn bộ lifecycle Kafka.
- [KIP-631 — Quorum-based Controller](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/158864865/KIP-631%2BThe%2BQuorum-based%2BKafka%2BController): registration, broker heartbeat, metadata records và UnregisterBroker.
- [KIP-778 — KRaft to KRaft Upgrades](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=188746840): metadata.version dựa trên feature versioning; không đồng nhất schema version với cluster feature level.
- [KIP-1073 — Return fenced brokers in DescribeCluster](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=315493269): bổ sung liệt kê fenced brokers, không phải nguồn giới thiệu API UnregisterBroker.

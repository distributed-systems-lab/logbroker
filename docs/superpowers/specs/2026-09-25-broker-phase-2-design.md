# Phase 2 — Broker đơn và Java client

Ngày: 2026-09-25.

Trạng thái: Người dùng đã duyệt toàn bộ bản spec ngày 2026-09-25, gồm các chi tiết và giá trị mặc định trước đó ghi là đề xuất. Implementation plan: [Phase 2](../plans/2026-09-25-broker-phase-2.md). Chưa triển khai Phase 2.

## 1. Mục tiêu và phạm vi

Biến thư viện persistent partition log của Phase 1 thành broker đơn phục vụ nhiều Java client qua TCP. Giữ nguyên các hợp đồng storage, đặc biệt append khác flush, recovery nghiêm ngặt và offset theo batch. Namespace: `vn.huyqt.logbroker`.

Bao gồm topic nhiều partition; metadata bền vững cục bộ; binary protocol riêng; CreateTopic, Metadata, Produce, Fetch; Netty transport; Java producer batching/routing; consumer đọc theo partition/offset; backpressure, timeout, long polling và shutdown có kiểm soát.

Không gồm KRaft quorum, replication, consumer groups, committed consumer offsets, ShareConsumer, deduplication, transactions, compression, retention, xóa topic, tăng số partition hoặc Kafka wire compatibility. TCP chưa có TLS/authentication; phạm vi sử dụng là môi trường lab tin cậy. Phase này chưa đưa ra bảo đảm replication hoặc exactly-once.

## 2. Các quyết định đã chốt

| Nội dung | Quyết định |
| --- | --- |
| Produce acknowledgment | APPENDED hoặc FLUSHED, chung cho request |
| Flush | Theo từng partition, kích hoạt bởi chu kỳ hoặc ngưỡng byte |
| Fetch visibility | Đến logEndOffset, bao gồm batch chưa flush |
| Request | Nhiều partition; kết quả riêng; không atomic giữa partition |
| Fetch chờ | Long polling với minBytes và maxWaitMs |
| Metadata | Local metadata log; topic ID từ Phase 2 |
| Producer routing | Partition chỉ định, nếu không hash key, key null round-robin theo batch |
| Produce retry | Không tự retry khi kết quả không rõ |
| Broker execution | Pool giới hạn; hàng đợi tuần tự riêng theo partition |
| Connection | Nhiều request in-flight; response có thể khác thứ tự |
| Batch trên mạng | Format riêng, dùng chung encoding record với storage |
| Fetch budget | Chỉ batch đầu toàn response được vượt budget để tiến triển |
| Failure isolation | Cô lập data partition lỗi; metadata lỗi là lỗi toàn broker |

## 3. Thành phần và ranh giới

| Thành phần | Trách nhiệm |
| --- | --- |
| Protocol codec | Encode/decode envelope, operation và wire batch; validation có giới hạn |
| Netty transport | Connection, framing, gửi/nhận và điều tiết đọc/ghi |
| Request dispatcher | Kiểm tra request, admission, tách/ghép kết quả nhiều partition |
| Partition executor | Serialize append/read/flush cho mỗi partition trên pool chung |
| Flush coordinator | Theo dõi dirty bytes/thời hạn và hoàn thành FLUSHED waiters |
| Fetch coordinator | Quản lý long polls, deadline và thông báo thay đổi partition |
| Metadata service | Serialize CreateTopic, ghi metadata và áp dụng catalog |
| Partition registry | Mở/đóng PartitionLog, theo dõi trạng thái khả dụng |
| Java client | Metadata lookup, routing, batching, correlation, timeout và bounded buffers |

Core không nhận Channel, ByteBuf hay kiểu Netty khác. Ranh giới transport sử dụng message và dữ liệu Java do bên nhận sở hữu; không để buffer pool đã giải phóng lọt vào tác vụ nền. Codec không phụ thuộc Netty. Sau này transport Java NIO phải qua cùng conformance tests. Chưa thiết kế zero-copy hoặc framework transport tổng quát.

Giữ Maven project hiện có trong Phase 2; tổ chức theo package protocol, broker, client và transport.netty, storage tiếp tục độc lập. Có thể tách phần codec record dùng chung nhưng không thay đổi byte format storage v1. Phiên bản Netty và API lớp cụ thể sẽ được kiểm tra khi viết implementation plan.

## 4. Metadata và topic lifecycle

Mỗi topic có tên duy nhất, UUID ổn định và số partition cố định; partition ID là 0 đến count - 1. Produce/Fetch định danh bằng topic ID và partition ID. Metadata trả ánh xạ tên/ID, số partition, endpoint broker và trạng thái khả dụng từng partition. Đề xuất tên topic chỉ chứa ASCII chữ, số, dấu chấm, gạch ngang, gạch dưới; dài 1–249 ký tự; loại trừ `.` và `..`. Không dùng tên topic trực tiếp làm đường dẫn.

Đề xuất layout: `metadata/` cho metadata PartitionLog; `topics/<topic-id>/<partition-id>/` cho data log. Metadata log không xuất hiện như user topic. Broker giữ khóa thư mục gốc trong suốt vòng đời; partition lock hiện tại vẫn giữ nguyên.

CreateTopic được serialize bởi metadata service: kiểm tra tên/count → cấp ID → append một sự kiện TopicCreated có version → flush metadata → áp dụng catalog → khởi tạo partition → trả thành công khi tất cả partition sẵn sàng. Không expose topic từ metadata chưa flush. Không phục vụ data trước khi topic được công bố trong catalog.

Startup replay các sự kiện hợp lệ, sau đó hoàn tất khởi tạo thư mục/partition còn thiếu. TopicCreated hợp lệ còn lại sau crash có thể được recovery giữ lại dù client chưa nhận thành công; mất phản hồi không chứng minh topic chưa tồn tại. Không tự xóa metadata khi khởi tạo data partition thất bại: catalog giữ topic và báo partition không khả dụng.

Đề xuất CreateTopic gọi lại cùng tên/count trả cùng ID và trạng thái hiện tại, chỉ thành công nếu topic sẵn sàng; cùng tên khác count trả TOPIC_ALREADY_EXISTS. Không tạo ID mới cho request lặp. Metadata dùng để kiểm tra kết quả sau timeout. Không tự thu nhận thư mục không được catalog tham chiếu; không tự xóa chúng.

Tách bộ áp dụng TopicCreated khỏi local append/flush. Phase 3–4 thay nguồn sự kiện bằng metadata đã commit của KRaft; không biến local metadata log thành consensus trong Phase 2. Topic ID được đưa lên sớm từ mốc Phase 4 của roadmap.

## 5. Protocol và wire batch

Protocol có frame length, operation ID, version theo operation, request ID và error code ổn định. Đề xuất envelope v1: big-endian, int32 frameLength không bao gồm chính trường length; int16 operation; int16 version; int64 requestId; body. Response echo operation/version/requestId. Mỗi direction có body riêng; error scope cấp request và cấp partition được phân biệt. Operation ID đề xuất: CreateTopic=1, Metadata=2, Produce=3, Fetch=4; version đầu tiên=1.

Length-prefix framing phải xử lý được frame chia nhỏ hoặc nhiều frame trong một lần đọc. Kiểm tra length trước cấp phát; count, string, batch và phép cộng kích thước phải chống overflow. Request hợp lệ về framing nhưng operation/version không hỗ trợ nhận lỗi tương ứng; framing hỏng hoặc không thể xác định envelope an toàn thì đóng kết nối. Body malformed bị từ chối trước dispatch; lỗi nghiệp vụ từng partition không hủy kết quả partition khác. Không chấp nhận partition trùng trong cùng request.

Wire batch Produce không có offset do client cấp. Đề xuất v1 gồm version int16, totalLength int32 bao gồm header, recordCount int32, CRC32C 4 byte và record payload; CRC bao phủ toàn wire batch trừ chính trường CRC. Record payload dùng đúng encoding timestamp/key/value/headers của Phase 1, giữ thứ tự và phân biệt null/rỗng. Broker kiểm tra checksum, count, length và kích thước storage batch tương ứng trước admission; một wire batch có thể vừa giới hạn mạng nhưng vẫn vượt giới hạn storage do header khác kích thước.

Fetch trả từng batch với baseOffset và wire batch tương ứng. Response không hứa tương thích raw bytes trên disk. Chưa có compression, attributes mở rộng hoặc zero-copy. Protocol version và storage version độc lập. Bảng byte layout đầy đủ của operation bodies và ánh xạ mã lỗi số phải được ghi trong tài liệu protocol trước khi viết codec ở implementation plan; không suy ra wire format từ Java serialization.

Các nhóm lỗi bắt buộc: INVALID_REQUEST, UNSUPPORTED_OPERATION, UNSUPPORTED_VERSION, UNKNOWN_TOPIC, UNKNOWN_PARTITION, TOPIC_ALREADY_EXISTS, OFFSET_OUT_OF_RANGE, BATCH_TOO_LARGE, OVERLOADED, REQUEST_TIMED_OUT, PARTITION_UNAVAILABLE, STORAGE_ERROR, BROKER_SHUTTING_DOWN. Thông báo mô tả không thay thế mã lỗi và không yêu cầu client parse text.

## 6. Produce, flush và durability

Mỗi request chứa một batch cho mỗi partition, acknowledgment mode chung và processing timeout. Mỗi kết quả partition thành công trả firstOffset/nextOffset exclusive. Không atomic giữa các partition. Response được ghép khi mọi partition có kết quả hoặc hết deadline; các kết quả đã biết được giữ nguyên.

APPENDED thành công sau khi PartitionLog.append hoàn tất. FLUSHED thành công khi durableEndOffset >= nextOffset của batch. Không giữ worker trong khi chờ FLUSHED. Nếu append/rollover đã làm durable marker tiến lên, đánh giá ngay các waiters đã được bao phủ, không bắt buộc thêm một flush chỉ để xác nhận chúng.

Partition có dữ liệu chưa flush được đánh dấu dirty. Flush được lên lịch khi dirty encoded storage bytes đạt flushBytes HOẶC tuổi của batch chưa flush lâu nhất đạt flushInterval. Append mới không dời thời hạn cũ. APPENDED và FLUSHED đều tạo dirty data. Mỗi partition tối đa một flush task đang chờ/đang chạy; flush đến hạn có ưu tiên trước data task đang xếp hàng, không ngắt I/O đang chạy. Metadata flush phục vụ CreateTopic là trực tiếp, không đợi lịch flush data.

Sau force thành công, cập nhật trạng thái dirty theo durableEndOffset thực tế và hoàn thành các waiters được bao phủ. Rollover có thể force prefix nhưng để batch mới vẫn dirty; không reset nhầm toàn bộ bộ đếm/thời hạn. Với hàng đợi tuần tự, không có append đồng thời trong chính partition khi flush chạy.

Lỗi write/force áp dụng FAILED của Phase 1, partition không tiếp tục phục vụ cho đến close/reopen. Các FLUSHED waiter chưa hoàn thành nhận lỗi; không đảo ngược APPENDED đã trả thành công. Độ trễ flush có thể vượt chu kỳ do I/O hoặc tải; chu kỳ là ngưỡng lên lịch.

Kế thừa toàn bộ giả định force/filesystem và giới hạn directory-entry durability từ Phase 1. durableEndOffset không phải replication high watermark. Process-kill test không chứng minh an toàn trước mất điện. Phase 2 không cho client gọi truncate.

## 7. Fetch, giới hạn byte và long polling

Fetch chỉ rõ topic ID, partition ID, offset, partitionMaxBytes; cấp request có maxBytes, minBytes, maxWaitMs. Đề xuất yêu cầu maxBytes và partitionMaxBytes > 0; 0 <= minBytes <= maxBytes; maxWaitMs >= 0 và có trần cấu hình. maxWaitMs=0 hoặc minBytes=0 trả ngay dữ liệu hiện có.

Đọc đến logEndOffset, không đợi durableEndOffset. Offset tại end trả rỗng; ngoài [start,end] trả OFFSET_OUT_OF_RANGE. Response trả logStartOffset/logEndOffset và kết quả từng partition. Không bảo đảm một snapshot atomic xuyên partition. Batch đầu một partition có thể bắt đầu trước offset yêu cầu; Java client lọc các record trước offset đó.

Budget đo theo tổng encoded wire batch bytes, gồm baseOffset của từng batch, không tính envelope hay metadata partition. maxWireBatchBytes dùng trong công thức bên dưới cũng gồm baseOffset. Chỉ batch dữ liệu đầu tiên của toàn response được vượt partitionMaxBytes hoặc maxBytes; nếu dùng ngoại lệ, không thêm batch dữ liệu nào nữa. Các batch sau phải vừa cả hai ngân sách còn lại. Tổng byte batch <= max(maxBytes,maxWireBatchBytes). Frame cap vẫn cứng và phải chừa đủ chỗ cho envelope cùng mọi partition result được phép; cấu hình không tương thích phải bị từ chối.

Xét partition theo thứ tự request; Java client luân phiên thứ tự giữa các lần Fetch. Partition chưa được cấp budget trả rỗng thành công, không mang nghĩa đã hết log. Khi chuyển từ budget mạng sang storage read budget phải kiểm tra lại kích thước wire trước ghép response, vì hai format khác nhau.

minBytes tính trên batch bytes thực sự có thể trả sau áp dụng các budget. Nếu chưa đủ, đăng ký long poll và nhả worker/lock; khi có append, lỗi partition hoặc hết maxWaitMs thì đánh giá lại. Khi hết hạn trả dữ liệu hiện có dù chưa đạt minBytes; nếu có lỗi partition trả sớm kết quả hiện có. Hết maxWaitMs bình thường không phải lỗi timeout.

Đăng ký waiter phải có kiểm tra lại generation/end offset sau đăng ký để không bỏ lỡ append giữa lần đọc và đăng ký. Sự kiện được gộp để một waiter không tạo vô hạn tác vụ đọc lại. Không giữ bản sao response lớn trong suốt thời gian chờ; lần đánh giá cuối vẫn qua admission bộ nhớ. Disconnect/deadline/shutdown gỡ waiter đúng một lần.

## 8. Thực thi, thứ tự và backpressure

Append/read/flush được serialize theo partition trên pool có số thread giới hạn. Mỗi lần chạy xử lý một tác vụ rồi nhường pool; không drain vô hạn một partition. Netty event loop không thực hiện storage I/O hoặc chờ future. Metadata có luồng thực thi tuần tự riêng để data queue không chặn CreateTopic vô hạn.

Produce cùng partition trên cùng connection giữ thứ tự nhận khi enqueue, kể cả khi validation được chuyển sang worker. Có thể dùng stage tuần tự theo connection trước dispatch. Giữa connection, thứ tự append là thứ tự admission vào partition queue. Không cam kết thứ tự toàn topic hoặc thứ tự hoàn thành consumer.

Giới hạn số connection, frame bytes, partitions/request, tasks/partition, queued bytes toàn broker, waiters/connection và toàn broker, cũng như pending outbound bytes. Reserve budget trước cấp phát/queue khi biết kích thước; giải phóng chính xác khi hoàn thành/hủy. Bộ đếm phải tính cả object/record overhead bằng giới hạn record count, không chỉ payload bytes. Không cho checksum/decoding nặng chiếm event loop vô hạn; validation có hàng đợi giới hạn.

Hết admission budget trước enqueue trả OVERLOADED cho phần việc chưa nhận. Không drop dữ liệu đã append để giải phóng queue. Slow reader bị ngừng nhận thêm request hoặc đóng connection khi vượt ngưỡng outbound; Produce đang xử lý có thể tiếp tục và kết quả phía client không rõ. Flush/control completion có đường thực thi dành riêng, không bị từ chối chỉ vì user queue đầy.

## 9. Java client và correlation

API bất đồng bộ dùng CompletableFuture, có wrapper đồng bộ nếu cần. Producer nhận record riêng lẻ, gom theo topic ID/partition; đóng batch khi đạt byte/count cap hoặc linger deadline. Một Produce request gom tối đa một batch mỗi partition; batch tiếp theo của cùng partition được gửi theo thứ tự. Future từng record nhận offset tương ứng từ kết quả batch.

Routing: partition chỉ định được ưu tiên; nếu không, key khác null được hash; key null round-robin theo batch. Đề xuất hash là CRC32C trên raw key bytes, chuyển thành unsigned long rồi modulo partition count, có golden-vector tests; đây là thuật toán riêng, không cam kết tương thích partitioner Kafka. Counter round-robin riêng mỗi topic trong mỗi producer; key rỗng vẫn được hash. Số partition không thay đổi trong Phase 2.

Batching thực hiện trước wire encode; record vượt storage/wire cap bị từ chối riêng trước gửi. Producer buffer hữu hạn: mặc định từ chối send mới bằng lỗi local khi đầy thay vì chặn vô hạn. Một record vượt giới hạn hoặc chưa admission không làm mất các record hợp lệ đã nhận.

Client có Metadata lookup theo tên và cache topic ID. Consumer là API fetch tường minh partition/offset, trả record và thông tin để caller quyết định offset tiếp theo; không tự commit, không group coordination. Không tự bỏ qua offset lỗi hoặc reset về đầu/cuối log.

Nhiều request có thể in-flight trên một connection. requestId tăng đơn điệu, không tái sử dụng trong vòng đời connection; trước overflow đóng và tạo connection mới. Pending table gắn cả connection generation; response muộn không thể hoàn thành future mới. Không tự retry Produce, kể cả reconnect. Metadata lookup có thể được ứng dụng gọi lại; Phase 2 không cần một retry framework.

Timeout tính bằng monotonic clock. Client deadline bao gồm batching và gửi/chờ; broker processing deadline bắt đầu sau khi nhận đủ và admit request, có tính thời gian queue. Trước khi bắt đầu ghi socket, client có thể phân loại NOT_SENT; từ khi bắt đầu ghi frame phải coi UNKNOWN nếu không có kết quả xác định. requestId chỉ correlation, không deduplication.

Broker bỏ qua tác vụ đã hết deadline nếu chưa bắt đầu storage mutation. Timeout sau khi mutation có thể đã bắt đầu trả REQUEST_TIMED_OUT với kết quả không rõ; không hứa cancel I/O đang chạy. Lỗi I/O sau mutation cũng có thể để lại dữ liệu recovery giữ lại. Với request nhiều partition, giữ kết quả riêng đã biết; không retry cả request ngầm. Hủy future không chứng minh broker đã hủy ghi.

Producer.close ngừng nhận send, gửi các batch đã nhận và chờ future trong timeout cấu hình, sau đó đóng connection và báo các kết quả chưa rõ. Không nâng APPENDED thành FLUSHED khi close; độ bền vẫn theo mode đã chọn.

## 10. Vòng đời và cô lập lỗi

Startup: khóa data root → recover metadata → áp dụng catalog → mở/khôi phục các data partition → mở listener. Metadata corruption ngăn startup. Data partition lỗi được đánh dấu unavailable, broker vẫn phục vụ phần khỏe. Không tự retry recovery liên tục hoặc sửa data corruption trái với Phase 1.

Lỗi metadata append/force khi đang chạy làm broker ngừng admission và shutdown có kiểm soát. Topic có partition khởi tạo lỗi không làm mất metadata đã lưu; trả lỗi và trạng thái rõ ràng. Data partition write/force lỗi chỉ cô lập partition tương ứng, hoàn thành các waiter liên quan bằng lỗi.

Shutdown bình thường: ngừng nhận request mới → kết thúc Fetch đang chờ bằng dữ liệu hiện có → drain tác vụ đã nhận → flush data → gửi kết quả còn lại → đóng storage và connection → nhả root lock. Có deadline chung; quá hạn đóng connection, best-effort giải phóng tài nguyên, không tuyên bố mọi append đã flush. Không đóng storage đồng thời thiếu phối hợp với I/O đang chạy. Process bị dừng sau deadline được recovery ở lần mở tiếp theo.

## 11. Giá trị cấu hình khởi đầu đề xuất

Các giá trị này phục vụ lab, không phải kết quả benchmark. Implementation plan phải thống nhất validation giữa client/broker trước khi viết codec.

| Cấu hình | Giá trị khởi đầu |
| --- | --- |
| Data flush | 10 ms hoặc 1 MiB storage bytes chưa flush |
| Storage limits | Giữ defaults Phase 1: segment 64 MiB, max batch 1 MiB |
| Wire batch cap, gồm baseOffset khi Fetch | 1 MiB; đồng thời kiểm tra storage cap |
| Frame cap | 8 MiB; Fetch batch budget tối đa 4 MiB |
| Partition entries/request | 64 |
| Record count/batch | 10.000, đồng thời tuân thủ byte cap |
| Producer batching | 64 KiB target, linger 5 ms; record lớn hợp lệ tạo batch riêng |
| Producer queued bytes | 16 MiB mỗi producer |
| In-flight requests | 32 mỗi connection |
| Client request deadline | 30 giây |
| Broker processing timeout tối đa | 30 giây |
| Fetch maxWaitMs mặc định / tối đa | 500 ms / 5 giây |
| Shutdown deadline | 30 giây |
| Data workers / validation workers | 4 / 2 |
| Connections toàn broker | 128 |
| User tasks chờ mỗi partition | 256 |
| Validation tasks chờ toàn broker | 256 |
| Queued/decoded request bytes toàn broker | 64 MiB, gồm cả request đang xử lý |
| Outbound response bytes mỗi connection / toàn broker | 16 MiB / 64 MiB |
| Request contexts còn sống toàn broker | 1.024, gồm active requests và long polls |
| FLUSHED partition waiters toàn broker | 4.096 |
| Topic count / tổng partition count | 128 / 1.024 |

Các hạn mức trên phải cấu hình được và có test chạm giới hạn. FLUSHED waiter capacity được reserve trước append để không ghi rồi mới phát hiện không đủ chỗ theo dõi xác nhận. Metadata list-all bị chặn bởi catalog caps; một response tối đa 1 MiB cho metadata/header và thông báo lỗi phải được giới hạn độ dài. Kết hợp vùng dự phòng này với Fetch budget 4 MiB và frame cap 8 MiB bảo đảm không vượt frame; cấu hình thay đổi phải được kiểm tra cùng nhau. Không dùng unbounded executor hoặc queue làm mặc định.

## 12. Kiểm chứng và điều kiện hoàn thành

- Protocol golden fixtures và round-trip; partial/coalesced frames, malformed lengths/counts, overflow, CRC sai, version/operation không hỗ trợ, duplicate partition và giới hạn frame trước allocation.
- Produce APPENDED không đợi force; FLUSHED không hoàn thành sớm; nhiều batch được một flush bao phủ; kích hoạt time/bytes độc lập; append liên tục không dời deadline; rollover không reset nhầm dirty state; force lỗi đánh thức waiter.
- Fetch đọc được dữ liệu chưa flush, lọc offset giữa batch, xử lý offset đầu/cuối/out-of-range; nhiều partition và giới hạn tổng; chỉ một oversized-first-batch exception, wire size khác storage size, thứ tự luân phiên không bỏ đói.
- Long poll đủ minBytes, hết hạn trả một phần/rỗng, append đúng thời điểm đăng ký, partition lỗi, disconnect và shutdown; không rò waiter hoặc giữ worker chờ.
- Metadata replay, CreateTopic đồng thời/trùng tên, mất response, crash sau metadata flush trước tạo đủ partition; metadata corruption chặn broker, data corruption cô lập partition; khóa root ngăn hai broker cùng data directory.
- Nhiều client/partition; thứ tự append cùng connection; response đảo thứ tự; timeout rồi response muộn; request ID không reuse; reconnect không gửi lại Produce; giữ partial success khi partition khác lỗi.
- Queue đầy trước mutation không ghi dữ liệu; client buffer đầy; slow reader; cap connection/waiter/decoded/outbound; budget được trả sau mọi error/cancel path; flush không bị starvation.
- Process crash và reopen xác minh record đã FLUSHED trong giả định Phase 1; không yêu cầu record APPENDED phải biến mất. Fault injection append/force/network xác minh UNKNOWN, không tự retry hay báo thành công sai.
- Shutdown sạch drain/flush, shutdown quá hạn và recovery; tất cả tests Phase 1 tiếp tục qua. Dùng clock điều khiển/latch cho deadline và race tests, tránh dựa vào sleep mong manh.

Hoàn thành khi có ví dụ chạy broker và các Java client riêng process để create topic, produce, fetch, restart; tài liệu protocol và cấu hình; kiểm thử tích hợp/fault injection trên qua `mvn clean verify`. Chưa yêu cầu benchmark hoặc implementation Java NIO thay Netty.

## 13. Ranh giới cho các phase sau

KRaft thay nguồn metadata cục bộ bằng metadata đã commit; partition data replication vẫn là leader/follower và không dùng Raft riêng mỗi data partition. Replication acknowledgment sau này độc lập với APPENDED/FLUSHED; giới hạn đọc theo high watermark cần được thiết kế tại Phase 5, không đổi tên durableEndOffset thành high watermark.

ShareFetch/ShareAcknowledge sẽ là operation riêng, không đổi nghĩa Fetch hiện tại. Record/storage không thêm group, owner, ACK hoặc acquisition lock. Consumer progress, membership và share state vẫn nằm ngoài Phase 2. Topic ID ổn định tạo ranh giới cho lifecycle topic ở các phase sau.

## Tham chiếu nội bộ

- [Roadmap](2026-09-24-broker-roadmap.md)
- [Phase 1 design](2026-09-24-storage-phase-1-design.md)
- [Storage format và API](../../storage-format-v1.md)

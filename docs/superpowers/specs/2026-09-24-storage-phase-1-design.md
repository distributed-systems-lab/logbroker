# Phase 1 — Persistent partition log

Trạng thái: Người dùng đã duyệt ngày 2026-09-25. Bước tiếp theo là lập kế hoạch triển khai.

## 1. Mục tiêu và quyết định đã chốt

Xây thư viện Java lưu trữ log của một partition, làm nền tảng cho broker theo kiến trúc Kafka/KRaft. Phase này chưa có networking, controller, replication hoặc consumer group.

Các quyết định người dùng đã chốt:

- Tách append và flush; append thành công chưa bảo đảm dữ liệu tồn tại sau mất điện.
- Lưu theo record batch ngay từ Phase 1.
- Recovery nghiêm ngặt: chỉ sửa phần đuôi ghi thiếu trong active segment; checksum sai trên batch đầy đủ hoặc segment cũ hỏng phải báo lỗi.
- Storage không chứa trạng thái ACK, group, owner hoặc acquisition lock; hỗ trợ Share Groups về sau bằng lớp riêng.

Các mục còn lại đã được duyệt cùng bản thiết kế; thay đổi hợp đồng trong quá trình triển khai phải được ghi lại và review.

## 2. Phạm vi

Bao gồm append, đọc theo offset, flush, segment rollover, sparse offset index, recovery, truncate theo ranh giới batch và đóng tài nguyên.

Chưa bao gồm compression, retention tự động, compaction, transaction, async I/O, memory mapping hoặc benchmark tối ưu. Giữ attributes để mở rộng định dạng sau; chưa hỗ trợ thuộc tính không biết.

Không cam kết tương thích binary format của Kafka. Bám theo mô hình batch/segment/offset, dùng định dạng riêng có version.

## 3. Thành phần và ranh giới

| Thành phần | Trách nhiệm |
| --- | --- |
| PartitionLog | Sở hữu segment, cấp offset, điều phối append/read/flush/recovery/truncate |
| LogSegment | Quản lý một data file và index tương ứng |
| BatchCodec | Encode/decode, validation và checksum; không biết filesystem hoặc consumer |
| OffsetIndex | Ánh xạ thưa từ offset đầu batch sang vị trí byte; có thể rebuild |
| LogRecovery | Kiểm tra chuỗi segment/batch, tạo kế hoạch sửa đuôi và rebuild index |

Dùng Java standard library cho file I/O và CRC32C. Không tạo abstraction thay thế mọi lời gọi filesystem. Chỉ đặt điểm fault injection nhỏ tại thao tác ghi/force cần kiểm thử.

## 4. Mô hình record và batch

Record gồm timestamp do caller cung cấp, key nullable, value nullable và danh sách header key/value. Phân biệt null và mảng byte rỗng. Header key là UTF-8, value là byte array nullable; cho phép trùng key và giữ thứ tự.

Batch header gồm magic, version, total length, base offset, record count, attributes và CRC32C. Payload chứa các record theo thứ tự. Phase 1 chỉ hỗ trợ batch không nén, attributes bằng không.

- Các số nhiều byte dùng big-endian; offset và timestamp dùng signed 64-bit.
- Length/count có encoding và giới hạn xác định; kiểm tra trước khi cấp phát, kiểm tra overflow khi cộng độ dài hoặc offset.
- CRC bao phủ toàn batch trừ chính trường CRC.
- Record tại vị trí i có offset baseOffset + i; không lưu offset lặp lại trong mỗi record.
- Batch rỗng bị từ chối; timestamp không dùng để quyết định thứ tự.
- Encode xong và kiểm tra kích thước trước khi thay đổi file hoặc cấp offset công khai.
- Header version 1 dài 30 byte, theo thứ tự: magic 4 byte (ASCII DLOG), version int16, totalLength int32 (bao gồm header), baseOffset int64, recordCount int32, attributes int32, CRC32C 4 byte. Record gồm timestamp int64, keyLength int32 và key bytes, valueLength int32 và value bytes, headerCount int32, rồi từng header với keyLength/key bytes/valueLength/value bytes. Length -1 chỉ biểu diễn null ở trường nullable; các length khác không âm. Decoder phải tiêu thụ đúng totalLength và recordCount, không chấp nhận byte dư.

## 5. API và concurrency

- open(directory, config): khóa độc quyền thư mục trong tiến trình/hệ điều hành, kiểm tra format và phục hồi trước khi phục vụ.
- append(records): ghi toàn bộ một batch; trả khoảng offset [firstOffset, nextOffset). Không force batch vừa append; rollover có thể force segment trước đó.
- read(offset, maxBytes): trả các batch hoàn chỉnh chứa offset yêu cầu trở đi; batch đầu có thể bắt đầu trước offset. Caller lọc record theo offset. Không có shared read cursor.
- flush(): trả durableEndOffset dạng exclusive sau khi force thành công các data file liên quan.
- truncateTo(offset): chỉ chấp nhận offset đầu batch hoặc logEndOffset; xóa từ offset đó trở đi. Nếu offset nằm giữa batch thì báo lỗi, không tự viết lại batch.
- close(): flush dữ liệu nếu log còn khỏe, đóng file và giải phóng khóa; lỗi flush phải được báo lại, vẫn cố giải phóng tài nguyên.

Log mới bắt đầu tại offset 0. Đọc với offset bằng logEndOffset trả rỗng; ngoài [logStartOffset, logEndOffset] trả lỗi. maxBytes phải dương, tính theo số byte encoded; thêm các batch hoàn chỉnh trong budget. Nếu batch đầu lớn hơn budget, chỉ trả batch đó để tránh không tiến triển. Tổng encoded bytes trả về không vượt max(maxBytes, maxBatchBytes).

Phase 1 dùng read/write lock: append, flush, rollover, truncate và close lấy write lock; read lấy read lock. Nhiều reader có thể chạy đồng thời, dùng positional reads để không tranh file cursor. Reader không thấy batch ghi dở. Đây là lựa chọn đơn giản ban đầu, chưa tối ưu độ trễ đọc khi ghi.

API trả dữ liệu độc lập với vòng đời file/channel; không đưa FileChannel hoặc buffer dùng chung có thể thay đổi cho caller.

## 6. Segment và index

Data file đặt tên theo base offset với độ rộng cố định; index cùng base name. Segment có base offset lớn nhất là active segment. Segment cũ không nhận append nhưng có thể bị loại khi truncate.

Cấu hình mặc định đề xuất: segment 64 MiB, max batch 1 MiB, sparse index interval 4 KiB. Kiểm tra maxBatchBytes không lớn hơn segmentBytes.

Batch không nằm trên hai segment. Nếu không đủ chỗ, force segment hiện tại rồi tạo segment mới với base offset bằng logEndOffset. Append kích hoạt rollover vì thế có thể phải đợi I/O đồng bộ.

Index lưu base offset và file position của các batch được chọn. Mỗi segment không rỗng có entry cho batch đầu; thêm entry khi khoảng cách byte đạt interval. Index không phải nguồn dữ liệu chuẩn: thiếu, ghi dở hoặc sai đều rebuild từ data log đã kiểm tra.

Không dùng index để kết luận data log hợp lệ trong recovery.

## 7. Durability và lỗi I/O

append thành công nghĩa là write loop đã ghi đủ batch vào file theo hợp đồng hệ điều hành, chưa phải persistence trên thiết bị. flush force dữ liệu và metadata file cần thiết trước khi tăng durableEndOffset. Index không cần là dữ liệu bền vững vì có thể rebuild.

flush giữ write lock nên phạm vi bao gồm mọi append thành công trước khi flush lấy lock. Phân biệt logEndOffset với durableEndOffset; cả hai là mốc exclusive. durableEndOffset không phải replication high watermark.

Sau open/recovery, force các file hợp lệ trước khi công bố durableEndOffset bằng logEndOffset của log phục hồi. Không suy ra lịch sử flush từ các byte tình cờ còn tồn tại.

Nếu write/force thất bại sau khi bắt đầu thay đổi file, đánh dấu log FAILED; từ chối read/append/flush bình thường cho đến khi đóng và mở lại để recovery. Validation lỗi trước I/O không làm log FAILED. Offset cấp cho append thất bại không được báo là thành công.

Giả định durability: local filesystem và thiết bị thực hiện đúng force. Không cam kết chống mọi lỗi phần cứng. Việc tồn tại của file mới sau mất điện còn phụ thuộc durability của directory entry; Java không cung cấp bảo đảm portable đồng nhất cho directory fsync. Phase 1 công bố rõ giới hạn này, kiểm thử process crash và fault injection; không tuyên bố kiểm thử process kill chứng minh an toàn trước mất điện.

## 8. Recovery nghiêm ngặt

Quét segment theo base offset, kiểm tra version, header, kích thước, CRC, record count/payload và tính liên tiếp của offset. Segment cũ phải kết thúc tại ranh giới batch hợp lệ; không cho phép segment cũ rỗng. Active segment rỗng hợp lệ.

Chỉ được sửa trường hợp EOF giữa header hoặc payload của batch cuối trong active segment, với các trường đã đọc được không mâu thuẫn quy tắc format. Không dùng một total length vô lý làm bằng chứng ghi dở.

Batch đầy đủ sai CRC, magic/version/length không hợp lệ, offset bị nhảy/lặp hoặc data corruption ở segment cũ đều khiến open thất bại. Không bỏ qua batch, không tự cắt suffix trong các trường hợp này.

Quét và xác thực toàn bộ trước khi sửa đuôi; rebuild index sau khi data hợp lệ. Cảnh báo recovery phải ghi rõ file và số byte đuôi bị loại.

Giới hạn: một số hỏng header tạo ra độ dài hợp lệ nhưng vượt EOF có thể không phân biệt được với torn write chỉ bằng định dạng này. Chính sách không tuyên bố phát hiện mọi dạng corruption hoặc chứng minh đuôi bị cắt chưa từng được flush.

## 9. Truncate

Chỉ caller của cơ chế replication tương lai quyết định offset được phép truncate; storage không tự biết commit/high watermark. Không renumber record giữ lại.

Xóa segment từ cuối về đầu, rồi truncate segment chứa ranh giới và rebuild index. Thao tác được serialize với read/append/flush. Sau truncate thành công, logEndOffset bằng offset đích, mốc durability được điều chỉnh và force phần thay đổi còn lại.

Phase 1 không cam kết truncate nhiều file là atomic khi crash; caller phải retry về offset đích sau recovery. Recovery vẫn phải phát hiện cấu trúc không hợp lệ thay vì âm thầm phục vụ. Giới hạn directory durability ở mục 7 cũng áp dụng cho xóa file.

## 10. Kiểm chứng và điều kiện hoàn thành

- Round-trip batch: null/rỗng, headers, nhiều record, offset, version, checksum và giới hạn kích thước.
- Nhiều segment: rollover đúng ranh giới; đọc tại đầu/giữa/cuối batch; index lookup và rebuild.
- Recovery: cắt file tại từng vị trí trong batch cuối; làm hỏng CRC trên batch đầy đủ; hỏng segment cũ; header sai; offset không liên tiếp.
- Durability: kiểm tra thứ tự write/force và mốc durableEndOffset; short writes và lỗi write/force có kiểm soát.
- Process crash: subprocess ghi rồi bị dừng; reopen và xác minh prefix hợp lệ. Không yêu cầu mọi dữ liệu chưa flush phải biến mất.
- Concurrency: reader không thấy batch dở; append được serialize; directory không mở bởi hai writer.
- Truncate: tại ranh giới batch/segment, offset không hợp lệ và recovery sau gián đoạn.

Phase hoàn thành khi bộ kiểm thử trên qua, có ví dụ append/read/flush/reopen và tài liệu mô tả chính xác các bảo đảm cùng giới hạn. Chưa cần benchmark hoặc broker chạy mạng.

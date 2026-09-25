# Storage format v1 — persistent partition log

Phase 1 lưu một partition trong một thư mục. Đây là định dạng riêng của dự án, không tương thích binary với Kafka. Mọi số nhiều byte dùng big-endian. Offset và timestamp là signed 64-bit.

## File và cấu hình

Data segment có tên `%020d.log`, index cùng base offset có tên `%020d.index`. Segment đầu có base offset 0. Mỗi batch nằm trọn trong một segment; active segment có thể rỗng. `.lock` giữ khóa độc quyền thư mục trong suốt vòng đời `PartitionLog`.

`LogConfig` yêu cầu `segmentBytes >= maxBatchBytes >= 50` và `indexIntervalBytes > 0`. Mặc định lần lượt là 64 MiB, 1 MiB và 4 KiB. Khi mở lại, `maxBatchBytes` phải đủ lớn cho mọi batch đã lưu, nếu không recovery báo lỗi.

Index là các cặp `int64 baseOffset, int64 bytePosition` big-endian, mỗi entry 16 byte. Batch đầu của mỗi segment luôn được index; entry tiếp theo được thêm khi khoảng cách byte từ entry gần nhất đạt interval. Index có thể rebuild từ data log, nên index thiếu, ghi dở hoặc sai không làm mất record. Lỗi I/O khi không thể ghi lại index vẫn khiến open/append thất bại.

## Batch header

| Vị trí byte | Trường | Số byte | Giá trị v1 |
| --- | --- | ---: | --- |
| 0 | magic | 4 | ASCII `DLOG` |
| 4 | version | 2 | `1` |
| 6 | totalLength | 4 | Gồm cả header, 50..maxBatchBytes |
| 10 | baseOffset | 8 | Offset record đầu, không âm |
| 18 | recordCount | 4 | Dương |
| 22 | attributes | 4 | `0` |
| 26 | CRC32C | 4 | CRC của byte 0..25 và 30..cuối |
| 30 | records | biến độ dài | Theo thứ tự trong batch |

Record gồm `timestamp:int64`, `keyLength:int32`, key bytes, `valueLength:int32`, value bytes, `headerCount:int32`, rồi từng header. Header gồm `keyLength:int32`, UTF-8 key bytes, `valueLength:int32`, value bytes. Key của header bắt buộc và phải là UTF-8 hợp lệ; header trùng key được giữ đúng thứ tự. Key/value record và value header dùng length `-1` cho `null`; length `0` cho mảng byte rỗng. Các length âm khác bị từ chối. Batch không có record bị từ chối. Decoder kiểm tra CRC, giới hạn length/count/offset, UTF-8 và việc tiêu thụ hết payload.

Record thứ `i` có offset `baseOffset + i`. Offset trả từ `append` là khoảng `[firstOffset, nextOffset)` với `nextOffset` exclusive. Timestamp do caller cung cấp, không quyết định thứ tự log.

## API và durability

`append(records)` encode và kiểm tra batch trước khi sửa file, ghi hết batch rồi trả khoảng offset. Thành công nghĩa là write loop đã hoàn tất theo hợp đồng hệ điều hành; **chưa** cam kết tồn tại sau mất điện. `flush()` force các data file và trả `durableEndOffset` exclusive. Rollover force segment cũ trước khi tạo segment mới, nên một append có thể làm prefix trước đó bền vững. `durableEndOffset` không phải replication high watermark.

`read(offset, maxBytes)` nhận offset trong `[0, logEndOffset]`; đọc tại end trả rỗng. Kết quả là các batch nguyên vẹn và độc lập với file handle. Batch đầu có thể bắt đầu trước offset yêu cầu; caller lọc record trong batch. `maxBytes` phải dương. Nếu batch đầu lớn hơn budget, hàm vẫn trả batch đó để tiến lên; các batch sau chỉ được thêm khi tổng encoded bytes nằm trong budget. Tổng trả về không vượt `max(maxBytes, maxBatchBytes)`.

`truncateTo(offset)` chỉ chấp nhận đầu batch hoặc `logEndOffset`; giữa batch và ngoài range là lỗi trước khi sửa file. Truncate xóa các segment sau từ cuối về đầu, giữ segment chứa boundary, rebuild index và force data trước khi công bố end mới. Phase 1 không bảo đảm truncate trên nhiều file là atomic khi crash; caller cần mở lại rồi retry offset đích.

Trạng thái `OPEN` phục vụ API. Lỗi ghi/force sau mutation đưa log sang `FAILED`; mọi thao tác khác `close` bị từ chối, giữ nguyên cause I/O đầu tiên. Lỗi validation trước mutation không làm log `FAILED`. `close()` ở `OPEN` flush rồi giải phóng file/lock; `close()` ở `FAILED` chỉ cleanup, không thử flush. `close()` lặp lại an toàn. Một append thất bại khi ghi index sau khi data batch đã đủ có thể không trả thành công nhưng batch vẫn hiện sau restart/recovery.

## Recovery

Open giữ khóa thư mục trước khi scan. Recovery quét **toàn bộ** data segment theo offset, xác thực từng batch và tính liên tiếp; chỉ sau khi mọi data hợp lệ mới cắt đuôi active nếu cần, rebuild index và force data. Sau open thành công, `durableEndOffset` được công bố bằng end đã phục hồi; không suy ra lịch sử flush trước crash từ các byte tình cờ còn tồn tại.

| Tình huống | Xử lý |
| --- | --- |
| EOF giữa header/payload batch cuối trong active segment, prefix đã thấy hợp lệ | Cắt từ đầu batch đó và ghi cảnh báo có path/số byte bị loại |
| Active segment rỗng | Chấp nhận |
| Segment cũ rỗng hoặc có batch ghi thiếu | Báo `CorruptLogException`, không sửa data |
| Batch đủ byte nhưng CRC/payload sai | Báo `CorruptLogException`, không sửa data |
| Magic, version, attributes, length, offset hoặc tên data file sai | Báo `CorruptLogException`, không sửa data |
| Index thiếu hoặc hỏng | Rebuild từ data đã xác thực |

Một header hỏng nhưng vẫn khai báo length hợp lệ vượt EOF có thể không phân biệt chắc chắn với ghi dở. Recovery không khẳng định mọi corruption đều được phát hiện, hoặc đuôi bị cắt chưa từng được flush.

Giả định durability là filesystem cục bộ và thiết bị thực hiện đúng `FileChannel.force(true)`. Java không cung cấp bảo đảm portable đồng nhất cho `fsync` của directory entry: file mới hoặc file bị xóa có thể chịu giới hạn persistence riêng của filesystem/OS. Test process kill kiểm tra recovery sau tiến trình chết, **không** mô phỏng mất điện. Phase này chưa có networking, replication, consumer group, compression, retention tự động hay transaction.

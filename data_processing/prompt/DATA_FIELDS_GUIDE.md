# Data Fields Guide

Phạm vi tài liệu này dựa trên dữ liệu thực tế trong `data_ingestion/data`.

Quy ước tên folder thực tế:
- `historical_price`
- `updatelastprices`
- `updateorderbooks` (người dùng thường gọi ngắn là `updateorderbook`)
- `updatetrades` (người dùng thường gọi ngắn là `update_trade`)

## Cấu trúc file quan sát được

| Folder | Kiểu dữ liệu thực tế |
| --- | --- |
| `historical_price` | Chủ yếu là 1 JSON object cho 1 ngày. Một số file cũ dạng tháng như `2024-01-01.json` lại chứa mảng nhiều object ngày. |
| `updatelastprices` | 1 file/ngày/symbol, chứa nhiều JSON object nối tiếp nhau trong ngày. Phần lớn là JSONL; một số file như FPT là nhiều object pretty-print nối tiếp. |
| `updateorderbooks` | 1 file/ngày/symbol, chứa nhiều snapshot order book nối tiếp nhau trong ngày. |
| `updatetrades` | 1 file/ngày/symbol, chứa nhiều trade record nối tiếp nhau trong ngày. |

## `historical_price`

| Field | Giải thích ngắn | Quan hệ / ghi chú |
| --- | --- | --- |
| `date` | Ngày giao dịch của bản ghi daily. | Dạng ISO datetime, thường là `T00:00:00`. |
| `symbol` | Mã cổ phiếu. | Khớp với tên folder symbol. |
| `priceOpen` | Giá mở cửa. | Giá đầu phiên trong ngày. |
| `priceHigh` | Giá cao nhất ngày. | Luôn >= `priceLow`. |
| `priceLow` | Giá thấp nhất ngày. | Luôn <= `priceHigh`. |
| `priceClose` | Giá đóng cửa ngày. | Thường khớp với snapshot cuối ngày của `updatelastprices.last_price` trong sai số float. |
| `priceBasic` | Giá cơ sở / giá tham chiếu ngày. | Suy ra từ tên field và cách dùng thực tế. |
| `priceAverage` | Giá khớp bình quân trong ngày. | Gần như luôn khớp `((totalValue - putthroughValue) / dealVolume / unit)`. |
| `totalVolume` | Tổng khối lượng giao dịch ngày. | Gần như luôn bằng `dealVolume + putthroughVolume`. |
| `dealVolume` | Khối lượng khớp lệnh. | Khớp rất tốt với `updatelastprices.deal_volume` cuối ngày. |
| `putthroughVolume` | Khối lượng thỏa thuận. | Phần khối lượng ngoài khớp lệnh liên tục/định kỳ. |
| `totalValue` | Tổng giá trị giao dịch ngày. | Bao gồm cả khớp lệnh và thỏa thuận. |
| `putthroughValue` | Giá trị giao dịch thỏa thuận. | Có thể suy ra `dealValue xấp xỉ totalValue - putthroughValue`. |
| `buyForeignQuantity` | Khối lượng mua của NĐT nước ngoài. | Có thể suy ra giá mua bình quân ngoại từ `buyForeignValue / buyForeignQuantity / unit` nếu quantity > 0. |
| `buyForeignValue` | Giá trị mua của NĐT nước ngoài. | Giá trị tiền, không phải giá đơn vị. |
| `sellForeignQuantity` | Khối lượng bán của NĐT nước ngoài. | Tương tự phía mua. |
| `sellForeignValue` | Giá trị bán của NĐT nước ngoài. | Tương tự phía mua. |
| `buyCount` | Số lệnh/bản ghi phía mua trong daily snapshot. | Ý nghĩa chi tiết do nguồn cung cấp; quan sát thấy đây không phải khối lượng. |
| `buyQuantity` | Tổng khối lượng phía mua trong daily snapshot. | Thường lớn hơn `totalVolume`, nên không phải riêng khối lượng khớp. |
| `sellCount` | Số lệnh/bản ghi phía bán trong daily snapshot. | Ý nghĩa chi tiết do nguồn cung cấp. |
| `sellQuantity` | Tổng khối lượng phía bán trong daily snapshot. | Thường lớn hơn `totalVolume`, nên không phải riêng khối lượng khớp. |
| `adjRatio` | Hệ số điều chỉnh giá. | Suy ra từ tên field; dùng cho dữ liệu điều chỉnh/corporate actions. |
| `currentForeignRoom` | Room ngoại hiện tại/còn lại tại thời điểm daily snapshot. | Cùng ý nghĩa business với field realtime trong `UpdateForeignerStats`. |
| `propTradingNetDealValue` | Giá trị tự doanh ròng phần khớp lệnh. | Dấu `+` là mua ròng, `-` là bán ròng. |
| `propTradingNetPTValue` | Giá trị tự doanh ròng phần thỏa thuận. | `PT` quan sát phù hợp với `putthrough`. |
| `propTradingNetValue` | Tổng giá trị tự doanh ròng. | Bằng `propTradingNetDealValue + propTradingNetPTValue`. |
| `unit` | Hệ số quy đổi giá sang tiền. | Quan sát hiện tại là `1000.0`; thường dùng trong công thức `price * volume * unit = value`. |

### Quan hệ quan sát được trong `historical_price`

- `propTradingNetValue = propTradingNetDealValue + propTradingNetPTValue`
  Ghi nhận đúng trên toàn bộ dữ liệu đã quét.
- `totalVolume = dealVolume + putthroughVolume`
  Đúng gần như toàn bộ dữ liệu; có 28 ngoại lệ, tập trung ở batch ngày `2025-07-16` trong các file tháng `2025-07-01.json`.
- `priceAverage ~= (totalValue - putthroughValue) / dealVolume / unit`
  Đúng gần như toàn bộ dữ liệu; có 15 ngoại lệ, cũng tập trung ở batch `2025-07-16`.
- `dealValue` không được lưu trực tiếp trong folder này, nhưng có thể suy ra:
  `dealValue ~= totalValue - putthroughValue`

## `updatelastprices`

| Field | Giải thích ngắn | Quan hệ / ghi chú |
| --- | --- | --- |
| `ts` | Thời điểm hệ thống ghi record. | Do writer thêm vào lúc lưu file; khác với `event_time`. |
| `command` | Tên command nguồn. | Luôn là `UpdateLastPrices`. |
| `symbol` | Mã cổ phiếu. | Khớp tên folder symbol. |
| `event_time` | Thời điểm sự kiện thị trường từ source. | Có timezone `+00:00`. |
| `last_price` | Giá khớp mới nhất tại thời điểm snapshot. | Đây là trường giá khớp intraday được pipeline sử dụng. |
| `last_volume` | Khối lượng của lần khớp mới nhất. | Là khối lượng incremental của trade cuối cùng, không phải lũy kế ngày. |
| `deal_volume` | Khối lượng khớp lệnh lũy kế trong ngày đến `event_time`. | Khớp cuối ngày với `historical_price.dealVolume`. |
| `deal_value` | Giá trị khớp lệnh lũy kế trong ngày đến `event_time`. | Xấp xỉ phần khớp lệnh của daily snapshot. |
| `matched_price` | Trường giá khớp phụ từ feed gốc. | Pipeline hiện tại bỏ qua trường này vì có case lệch so với `last_price`. |
| `active_buy_volume` | Khối lượng mua chủ động lũy kế. | Cộng với `active_sell_volume` ra `deal_volume`. |
| `active_sell_volume` | Khối lượng bán chủ động lũy kế. | Cộng với `active_buy_volume` ra `deal_volume`. |
| `total_buy_sale_volume` | Tổng khối lượng buy/sell chủ động lũy kế. | Quan sát hiện tại bằng `deal_volume`. |

### Quan hệ quan sát được trong `updatelastprices`

- `deal_volume = active_buy_volume + active_sell_volume`
  Đúng trên toàn bộ `39,989` record đã quét.
- `deal_value / deal_volume / 1000`
  Là giá khớp bình quân lũy kế tới thời điểm snapshot; thường khác `last_price` vì đây là bình quân tích lũy.

## `updateorderbooks`

| Field | Giải thích ngắn | Quan hệ / ghi chú |
| --- | --- | --- |
| `ts` | Thời điểm hệ thống ghi record. | Timestamp ingest/save. |
| `command` | Tên command nguồn. | Luôn là `UpdateOrderBooks`. |
| `symbol` | Mã cổ phiếu. | Khớp tên folder symbol. |
| `event_time` | Thời điểm snapshot order book từ source. | Có timezone `+00:00`. |
| `total_bid_volume` | Tổng dư mua do source trả về. | Trong dữ liệu hiện tại quan sát thấy luôn bằng `0.0`. |
| `total_ask_volume` | Tổng dư bán do source trả về. | Trong dữ liệu hiện tại quan sát thấy luôn bằng `0.0`. |
| `bid_levels` | Danh sách các mức giá mua tốt nhất. | Hiện tại luôn có 3 level. `bid_levels[0]` là best bid. |
| `ask_levels` | Danh sách các mức giá bán tốt nhất. | Hiện tại luôn có 3 level. `ask_levels[0]` là best ask. |
| `bid_levels[].price` | Giá tại từng mức bid. | Thứ tự giảm dần theo độ ưu tiên. |
| `bid_levels[].volume` | Dư khối lượng tại từng mức bid. | Không bằng `total_bid_volume` trong dataset hiện tại vì total đang luôn 0. |
| `ask_levels[].price` | Giá tại từng mức ask. | Thứ tự tăng dần theo độ ưu tiên. |
| `ask_levels[].volume` | Dư khối lượng tại từng mức ask. | Không bằng `total_ask_volume` trong dataset hiện tại vì total đang luôn 0. |

### Quan hệ quan sát được trong `updateorderbooks`

- `bid_levels` và `ask_levels` hiện tại luôn có đúng 3 mức giá trong toàn bộ dữ liệu đã quét.
- `total_bid_volume` và `total_ask_volume` hiện tại luôn bằng `0.0` trong `95,841` snapshot đã quét.
- Spread tốt nhất có thể suy ra từ:
  `best_spread = ask_levels[0].price - bid_levels[0].price`

## `updatetrades`

| Field | Giải thích ngắn | Quan hệ / ghi chú |
| --- | --- | --- |
| `ts` | Thời điểm hệ thống ghi record. | Timestamp ingest/save. |
| `command` | Tên command nguồn. | Luôn là `UpdateTrades`. |
| `symbol` | Mã cổ phiếu. | Khớp tên folder symbol. |
| `trade_id` | ID giao dịch từ source. | Dùng để phân biệt từng trade. |
| `event_time` | Thời điểm trade xảy ra theo source. | Có timezone `Z`/UTC. |
| `price` | Giá của trade. | Là giá đơn vị, chưa nhân `unit`. |
| `volume` | Khối lượng của trade. | Khối lượng incremental của đúng trade đó. |
| `side` | Hướng trade theo source. | Quan sát có các giá trị `B`, `S`, hoặc `null`. |
| `trading_date` | Ngày giao dịch của trade. | Chuỗi ngày dạng `YYYY-MM-DD`. |

### Quan hệ quan sát được trong `updatetrades`

- `side` hiện có 3 trạng thái quan sát được: `B`, `S`, `null`.
- Có `98` record `side = null` trên tổng `40,624` trade record đã quét, nên không nên ép field này luôn có giá trị.
- Giá trị tiền của từng trade có thể suy ra:
  `trade_value ~= price * volume * 1000`

## Liên hệ chéo giữa các folder

- Cùng symbol và cùng ngày, snapshot cuối ngày của `updatelastprices.deal_volume` khớp `historical_price.dealVolume`.
  Kiểm tra trên `30/30` symbol ngày `2026-03-13`.
- Cùng symbol và cùng ngày, snapshot cuối ngày của `updatelastprices.last_price` khớp `historical_price.priceClose` trong sai số float.
  Kiểm tra trên `30/30` symbol ngày `2026-03-13`.
- Cùng symbol và cùng ngày:
  `historical_price.totalValue - historical_price.putthroughValue ~= updatelastprices.deal_value`
  Kiểm tra trên `30/30` symbol ngày `2026-03-13`, sai số tuyệt đối tối đa quan sát được là `37,080`.
- `ts` là thời điểm hệ thống ghi file, còn `event_time` là thời điểm sự kiện thị trường.
  Khi join dữ liệu realtime, nên ưu tiên dùng `event_time` cho phân tích thị trường và dùng `ts` cho kiểm tra pipeline/độ trễ ingest.

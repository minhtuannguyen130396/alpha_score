Bạn là kiến trúc sư hệ thống và kỹ sư dữ liệu cho một nền tảng dự đoán alpha chứng khoán.

Nhiệm vụ của bạn là thiết kế và mô tả chi tiết toàn bộ Layer 2 của hệ thống.

Bối cảnh hệ thống:
- Layer 1 đã có sẵn dữ liệu thô.
- Layer 2 là tầng xử lý dữ liệu trung gian, có nhiệm vụ biến raw data thành dữ liệu chuẩn hóa, feature và metadata chất lượng để Layer 3 sử dụng.
- Layer 2 KHÔNG được đưa ra quyết định mua/bán.
- Layer 2 chỉ được:
  1. làm sạch dữ liệu,
  2. chuẩn hóa dữ liệu,
  3. đồng bộ dữ liệu theo thời gian,
  4. xây dựng market state,
  5. tính feature,
  6. đánh giá chất lượng / độ tin cậy dữ liệu.

Dữ liệu đầu vào của Layer 2 gồm 3 nhóm chính:

1. Daily data:
- open
- high
- low
- close
- total volume
- foreign buy volume
- foreign sell volume
- historical daily data khoảng 200 ngày trước
- có thể có thêm turnover, value, benchmark context nếu cần

2. Bid/Ask log theo giây hoặc theo snapshot:
- timestamp
- bid prices
- bid sizes
- ask prices
- ask sizes
- dữ liệu top 1 / top 3 / top 5 / top 10 tùy nguồn

3. Tick data:
- timestamp
- trade price
- trade volume
- trade side nếu có
- nếu không có thì cho phép suy luận side từ book/tick rule

Mục tiêu đầu ra của Layer 2:
Layer 2 phải xuất ra dữ liệu chuẩn hóa tại từng timestamp tham chiếu, gồm 3 khối chính:
1. market_state
2. feature_store
3. feature_quality

Yêu cầu rất quan trọng:
- Mỗi row output gắn với một timestamp cụ thể.
- Nhưng feature trong row có thể được tính từ dữ liệu lịch sử phía trước timestamp đó, ví dụ 5 giây, 10 giây, 60 giây, 20 ngày, 200 ngày.
- Không được sử dụng dữ liệu tương lai.
- Layer 2 phải deterministic: cùng input thì phải cho ra cùng output.
- Phải tách rõ field nào là market state, field nào là feature, field nào là quality.
- RSI, MACD, MA20, MA200, EMA, ATR, Bollinger Bands là feature của Layer 2, không phải logic buy/sell.
- Layer 2 không được sinh tín hiệu giao dịch.

Hãy thực hiện đầy đủ các nội dung sau:

PHẦN 1 — Mô tả vai trò của Layer 2
- Giải thích rõ Layer 2 làm gì và không làm gì.
- Phân biệt Layer 2 với Layer 1 và Layer 3.
- Giải thích vì sao Layer 2 là tầng “data refinery” của hệ thống.

# PHẦN 2 — Input Architecture của Layer 2 theo Phase

## 2.1. Mục tiêu của phần này

PHẦN 2 mô tả kiến trúc input của Layer 2 theo luồng phase, không mô tả theo kiểu tĩnh “daily / bid-ask / tick” như một danh sách đầu vào cuối cùng.

Mỗi phase trong phần này phải trả lời rõ 5 câu hỏi:

1. nhận object nào từ phase trước hoặc từ storage
2. xử lý input ở mức nào
3. tạo ra object output nào
4. output đó sẽ được phase sau dùng như thế nào
5. đâu là observed input và đâu là reconstructed input

Trong kiến trúc này, Layer 2 dùng **4 raw source groups quan sát được trực tiếp** và **1 derived/internal group được tái dựng**:

- Observed raw groups:
  - `historical_price`
  - `updatelastprices`
  - `updateorderbooks`
  - `updatetrades`
- Derived/internal group:
  - deep book memory / reconstructed depth state

Kiến trúc input này phải giữ các nguyên tắc sau:

- dùng `event_time` làm thời gian thị trường chính
- dùng `ts` như ingest/save time để theo dõi độ trễ pipeline
- không dùng dữ liệu tương lai
- deterministic: cùng input thì cùng output
- tách rõ observed input và reconstructed input
- mọi reconstructed field phải đi kèm confidence hoặc quality flag

## 2.2. Tổng quan các phase trong PHẦN 2

| Phase | Tên phase | Vai trò | Output chính |
| --- | --- | --- | --- |
| Phase 0 | Raw source intake | Nạp 4 nguồn raw thực tế theo symbol/ngày và cắt intraday ngoài session | `raw_source_bundle` |
| Phase 1 | Schema normalization | Chuẩn hóa raw source về schema nội bộ thống nhất | `normalized_input_bundle` |
| Phase 2 | Input validation & consistency check | Kiểm tra rule từng nguồn và liên hệ chéo giữa các nguồn | `validated_input_bundle` |
| Phase 3 | Time alignment & reference indexing | Căn thời gian theo `reference_time` và dựng frame input không dùng tương lai | `aligned_input_frame` |
| Phase 4 | Book memory & deep reconstruction input | Tái dựng latent depth, wall memory và candidate fake-wall từ top-3 book | `deep_book_input_bundle` |
| Phase 5 | Layer-2 ready input package | Đóng gói input observed + reconstructed + quality để sang PHẦN 3 | `layer2_input_package` |

## 2.3. Phase 0 — Raw source intake

### 2.3.1. Mục tiêu

Phase này chỉ làm nhiệm vụ nạp observed raw input từ Layer 1 hoặc storage và tạo bundle raw ban đầu cho từng `symbol`/`trading_date`.

Phase này chưa làm feature engineering, chưa suy luận market state, chưa tạo tín hiệu. Tuy nhiên phase này được phép áp dụng **session gate tối thiểu** để loại các record intraday nằm ngoài giờ làm việc vì đó là điều kiện hợp lệ của input, không phải feature.

### 2.3.2. Input của phase

Phase này nhận đúng 4 raw source groups:

1. `historical_price`
2. `updatelastprices`
3. `updateorderbooks`
4. `updatetrades`

### 2.3.3. Xử lý chính

- Quét source theo `symbol` và `trading_date`.
- Với `historical_price`, input mục tiêu của Layer 2 được coi là **1 JSON object / 1 ngày / 1 symbol**. Các dạng file legacy chứa nhiều object theo tháng không thuộc input target của kiến trúc mới.
- Với 3 nguồn intraday, parse record raw nhưng **giữ nguyên field observed**.
- Dùng `event_time` làm market time, parse từ UTC rồi đổi sang `UTC+7`.
- Loại toàn bộ record intraday có `event_time` ngoài khoảng `09:00` đến `14:45` theo giờ `UTC+7`.
- `ts` không dùng để quyết định session; `ts` chỉ là ingest/save time.
- Không suy ra full depth từ `total_bid_volume` và `total_ask_volume` vì dữ liệu quan sát thực tế hiện tại luôn là `0.0`.
- Không ép `updatetrades.side` luôn có giá trị vì field này có thể là `B`, `S`, hoặc `null`.
- Giữ `updateorderbooks.bid_levels` và `ask_levels` đúng dạng top-3 observed levels.
- Ghi output thành file bundle độc lập theo từng `symbol`/`trading_date` để dễ xóa, rebuild hoặc backfill riêng.

### 2.3.4. Cấu trúc input theo từng raw group

#### G0.1 — Raw daily source

Nguồn: `historical_price`

Field chính:

- `date`
- `symbol`
- `priceOpen`
- `priceHigh`
- `priceLow`
- `priceClose`
- `priceBasic`
- `priceAverage`
- `totalVolume`
- `dealVolume`
- `putthroughVolume`
- `totalValue`
- `putthroughValue`
- `buyForeignQuantity`
- `buyForeignValue`
- `sellForeignQuantity`
- `sellForeignValue`
- `buyCount`
- `buyQuantity`
- `sellCount`
- `sellQuantity`
- `adjRatio`
- `currentForeignRoom`
- `propTradingNetDealValue`
- `propTradingNetPTValue`
- `propTradingNetValue`
- `unit`

#### G0.2 — Raw intraday cumulative source

Nguồn: `updatelastprices`

Field chính:

- `ts`
- `command`
- `symbol`
- `event_time`
- `last_price`
- `last_volume`
- `deal_volume`
- `deal_value`
- `matched_price`
- `active_buy_volume`
- `active_sell_volume`
- `total_buy_sale_volume`

Quan hệ observed quan trọng:

- `last_price = matched_price`
- `deal_volume = active_buy_volume + active_sell_volume`

#### G0.3 — Raw order book source

Nguồn: `updateorderbooks`

Field chính:

- `ts`
- `command`
- `symbol`
- `event_time`
- `total_bid_volume`
- `total_ask_volume`
- `bid_levels`
- `ask_levels`
- `bid_levels[].price`
- `bid_levels[].volume`
- `ask_levels[].price`
- `ask_levels[].volume`

Ràng buộc observed hiện tại:

- `bid_levels` có đúng 3 levels
- `ask_levels` có đúng 3 levels
- `total_bid_volume` và `total_ask_volume` hiện không usable để suy ra full depth

#### G0.4 — Raw trade tick source

Nguồn: `updatetrades`

Field chính:

- `ts`
- `command`
- `symbol`
- `trade_id`
- `event_time`
- `price`
- `volume`
- `side`
- `trading_date`

Ràng buộc observed hiện tại:

- `side` có thể là `B`, `S`, hoặc `null`
- `event_time` là market trade time
- `ts` là ingest/save time

### 2.3.5. Output của phase

Output là `raw_source_bundle`.

```json
{
  "bundle_type": "raw_source_bundle",
  "symbol": "FPT",
  "trading_date": "2026-03-13",
  "source_group_status": {
    "historical_price": true,
    "updatelastprices": true,
    "updateorderbooks": true,
    "updatetrades": true
  },
  "raw_daily_records": ["historical_price object"],
  "raw_lastprice_records": ["updatelastprices records in-session"],
  "raw_orderbook_records": ["updateorderbooks records in-session"],
  "raw_trade_records": ["updatetrades records in-session"]
}
```

### 2.3.6. Ý nghĩa output

`raw_source_bundle` là input raw đã được giới hạn theo session giao dịch nhưng chưa chuẩn hóa schema. Phase 1 dùng object này để đổi toàn bộ observed raw field sang naming convention và time model nội bộ.

## 2.4. Phase 1 — Schema normalization

### 2.4.1. Mục tiêu

Chuẩn hóa 4 raw groups về schema nội bộ thống nhất để các phase sau không phải xử lý trực tiếp tên field gốc hoặc format thời gian gốc của Layer 1.

### 2.4.2. Input của phase

Input là `raw_source_bundle`.

### 2.4.3. Xử lý chính

- Parse tất cả thời gian sang datetime có timezone rõ ràng.
- Tách riêng:
  - `event_time_utc`
  - `event_time_local`
  - `ingested_at_utc`
- Chuẩn hóa tên field về naming convention nội bộ dạng `snake_case`.
- Giữ đồng thời observed raw field quan trọng nếu cần trace/debug.
- Với `updateorderbooks`, chuẩn hóa `bid_levels` và `ask_levels` thành cấu trúc level thống nhất, ví dụ mỗi level có `level_no`, `price`, `volume`.
- Với `updatetrades.side`, không ép sang non-null; giữ nguyên `null` khi nguồn không cung cấp được hướng giao dịch.
- Với `historical_price`, ánh xạ toàn bộ daily metrics sang daily schema nội bộ.
- Chuẩn hóa `reference_trading_date_local` theo ngày địa phương `UTC+7`.

### 2.4.4. Output của phase

Output là `normalized_input_bundle`, bên trong chứa tối thiểu các object sau:

- `normalized_daily_input`
- `normalized_intraday_snapshot_input`
- `normalized_orderbook_input`
- `normalized_trade_tick_input`

### 2.4.5. Ý nghĩa output

`normalized_input_bundle` là lớp observed input đã có schema nội bộ thống nhất. Phase 2 chỉ cần đọc object chuẩn thay vì xử lý chi tiết từng raw field gốc.

### 2.4.6. Ví dụ output

```json
{
  "normalized_daily_input": {
    "symbol": "FPT",
    "trading_date": "2026-03-13",
    "open": 77.1,
    "high": 78.5,
    "low": 76.3,
    "close": 77.0,
    "reference_price": 77.7,
    "average_price": 77.30603922003101,
    "total_volume": 10601000.0,
    "deal_volume": 9026000.0,
    "putthrough_volume": 1575000.0,
    "total_value": 828470910000.0,
    "putthrough_value": 130706600000.0,
    "foreign_buy_quantity": 537451.0,
    "foreign_sell_quantity": 1188021.0,
    "prop_net_value": 8834960000.0,
    "unit": 1000.0
  },
  "normalized_intraday_snapshot_input": {
    "symbol": "FPT",
    "event_time_utc": "2026-03-13T06:00:10.437000+00:00",
    "event_time_local": "2026-03-13T13:00:10.437000+07:00",
    "ingested_at_utc": "2026-03-13T13:00:10.713148+00:00",
    "last_price": 77.5999984741211,
    "matched_price": 77.5999984741211,
    "last_volume": 500.0,
    "deal_volume": 5029700.0,
    "deal_value": 388524998656.0,
    "active_buy_volume": 2339200.0,
    "active_sell_volume": 2690500.0
  },
  "normalized_orderbook_input": {
    "symbol": "FPT",
    "event_time_local": "2026-03-13T13:00:10.437000+07:00",
    "bid_levels": [
      {"level_no": 1, "price": 77.5, "volume": 26500.0},
      {"level_no": 2, "price": 77.4, "volume": 131500.0},
      {"level_no": 3, "price": 77.3, "volume": 114500.0}
    ],
    "ask_levels": [
      {"level_no": 1, "price": 77.6, "volume": 15700.0},
      {"level_no": 2, "price": 77.7, "volume": 19300.0},
      {"level_no": 3, "price": 77.8, "volume": 56700.0}
    ]
  },
  "normalized_trade_tick_input": {
    "symbol": "FPT",
    "trade_id": 400477280,
    "event_time_utc": "2026-03-13T06:00:10.417000+00:00",
    "event_time_local": "2026-03-13T13:00:10.417000+07:00",
    "ingested_at_utc": "2026-03-13T13:00:11.456720+00:00",
    "price": 77.6,
    "volume": 600,
    "side": "S",
    "reference_trading_date_local": "2026-03-13"
  }
}
```

## 2.5. Phase 2 — Input validation & consistency check

### 2.5.1. Mục tiêu

Kiểm tra tính hợp lệ của từng source sau chuẩn hóa và kiểm tra tính nhất quán chéo giữa các source trước khi chúng được đưa vào time alignment.

### 2.5.2. Input của phase

Input là `normalized_input_bundle`.

### 2.5.3. Xử lý chính

Validation theo từng source:

- Daily:
  - `price_high >= price_low`
  - `total_volume ~= deal_volume + putthrough_volume`
  - `average_price ~= (total_value - putthrough_value) / deal_volume / unit`
  - `prop_net_value = prop_net_deal_value + prop_net_pt_value`
- Intraday cumulative:
  - `last_price = matched_price`
  - `deal_volume = active_buy_volume + active_sell_volume`
- Order book:
  - `bid_levels` có đúng 3 mức
  - `ask_levels` có đúng 3 mức
  - `total_bid_volume` và `total_ask_volume` bị đánh dấu unusable nếu vẫn bằng `0.0`
- Trade:
  - `trade_id` không null
  - `price > 0`
  - `volume > 0`
  - `side` được phép null

Validation chéo giữa các source:

- Snapshot cuối ngày của `updatelastprices.deal_volume` phải gần khớp `historical_price.dealVolume`
- Snapshot cuối ngày của `updatelastprices.last_price` phải gần khớp `historical_price.priceClose`
- `historical_price.totalValue - historical_price.putthroughValue` phải gần khớp `updatelastprices.deal_value` cuối ngày

Phase này đồng thời sinh `input_validation_report` và degraded flags để Phase 3-5 biết dữ liệu nào dùng được ở mức observed, dữ liệu nào chỉ dùng với cảnh báo.

### 2.5.4. Output của phase

Output là `validated_input_bundle`.

```json
{
  "validated_input_bundle": {
    "normalized_inputs": "...",
    "input_validation_report": {
      "daily_rules": {
        "price_high_gte_price_low": "pass",
        "total_volume_matches_components": "pass",
        "average_price_consistency": "pass",
        "prop_net_value_consistency": "pass"
      },
      "intraday_rules": {
        "last_price_equals_matched_price": "pass",
        "deal_volume_equals_active_buy_plus_sell": "pass"
      },
      "orderbook_rules": {
        "bid_has_exactly_3_levels": "pass",
        "ask_has_exactly_3_levels": "pass"
      },
      "cross_source_rules": {
        "eod_deal_volume_matches_daily": "pass",
        "eod_close_matches_daily_close": "pass"
      }
    },
    "degraded_flags": {
      "book_total_depth_unusable": true,
      "trade_side_nullable": true
    }
  }
}
```

### 2.5.5. Ý nghĩa output

`validated_input_bundle` giữ lại input đã chuẩn hóa nhưng gắn thêm báo cáo validation và degraded flags. Phase 3 vẫn có thể dùng dữ liệu pass hoặc degraded, nhưng không còn mơ hồ về chất lượng input.

## 2.6. Phase 3 — Time alignment & reference indexing

### 2.6.1. Mục tiêu

Tạo một khung input đồng bộ theo từng `reference_time` để toàn bộ market snapshot, order book, trade window và daily context cùng quy chiếu về một mốc thời gian thị trường duy nhất.

### 2.6.2. Input của phase

Input là `validated_input_bundle`.

### 2.6.3. Xử lý chính

- Chọn `reference_time` từ trục `event_time` của dữ liệu intraday.
- Dùng `event_time` làm market time axis chính; không dùng `ts` làm trục căn chỉnh.
- Với mỗi `reference_time`, lấy:
  - daily context của đúng `trading_date`
  - intraday cumulative snapshot gần nhất **không vượt** `reference_time`
  - order book snapshot gần nhất **không vượt** `reference_time`
  - trade ticks trong các cửa sổ lùi 1s, 5s, 10s, 60s, 5m trước `reference_time`
- Tính metadata tuổi dữ liệu:
  - `book_age_ms`
  - `tick_age_ms`
  - `intraday_snapshot_age_ms`
- Tạo index giúp các phase sau truy cập nhanh theo `symbol`, `reference_time`, `window`, `price`.

### 2.6.4. Output của phase

Output là `aligned_input_frame`.

```json
{
  "aligned_input_frame": {
    "symbol": "FPT",
    "reference_time": "2026-03-13T13:00:10.437000+07:00",
    "daily_context": "normalized_daily_input",
    "intraday_snapshot": "latest normalized_intraday_snapshot_input <= reference_time",
    "orderbook_snapshot": "latest normalized_orderbook_input <= reference_time",
    "trade_windows": {
      "1s": ["ticks in (t-1s, t]"],
      "5s": ["ticks in (t-5s, t]"],
      "10s": ["ticks in (t-10s, t]"],
      "60s": ["ticks in (t-60s, t]"],
      "5m": ["ticks in (t-5m, t]"]
    },
    "age_metadata": {
      "book_age_ms": 0,
      "tick_age_ms": 20,
      "intraday_snapshot_age_ms": 0
    }
  }
}
```

### 2.6.5. Ý nghĩa output

`aligned_input_frame` là observed input đã được đưa về đúng một `reference_time`, sẵn sàng cho market-state construction, feature calculation và deep-book reconstruction mà không dùng dữ liệu tương lai.

## 2.7. Phase 4 — Book memory & deep reconstruction input

### 2.7.1. Mục tiêu

Tạo lớp input nội bộ để ước lượng phần depth không còn nằm trong top-3 observed book. Đây là phase tái dựng input, không phải raw observed source.

Phase này là bắt buộc vì:

- `updateorderbooks` hiện chỉ có đúng top-3 levels
- không có full depth thật
- `total_bid_volume` và `total_ask_volume` hiện không usable để suy ra depth thật
- cần giữ memory của các mức giá từng xuất hiện để theo dõi wall behavior

### 2.7.2. Input của phase

Input là `aligned_input_frame` cộng với memory state được carry-forward từ các `reference_time` trước đó của cùng `symbol`.

### 2.7.3. Xử lý chính

- Khi một mức giá từng xuất hiện trong top-3 rồi biến mất khỏi top-3, **không xóa ngay**.
- Chuyển mức đó sang trạng thái carry-forward hoặc latent level.
- Nếu mức giá xuất hiện lại, so sánh volume cũ và volume mới để đo mức độ reappearance.
- Từ order book transition, tính:
  - `disappeared_volume`
  - `last_observed_volume`
  - `carry_forward_volume`
- Từ `updatetrades`, gom `executed_volume_at_price` tại cùng mức giá và phía phù hợp.
- Tính `trade_explained_ratio`:
  - nếu phần volume biến mất chủ yếu được trade giải thích, khả năng cao level đã bị khớp
  - nếu phần volume biến mất hầu như không được trade giải thích, có thể suy luận cancel/pull
- Gắn state và quality:
  - `OBSERVED_TOP3`
  - `CARRIED_OUTSIDE_TOP3`
  - `REAPPEARED`
  - `CONSUMED_BY_TRADES`
  - `PULLED_OR_CANCELLED`
  - `SUSPECTED_FAKE_WALL`
  - `EXPIRED`

### 2.7.4. Output của phase

Output là `deep_book_input_bundle`.

```json
{
  "deep_book_input_bundle": {
    "symbol": "FPT",
    "reference_time": "2026-03-13T13:00:10.437000+07:00",
    "memory_levels": [
      {
        "symbol": "FPT",
        "side": "bid",
        "price": 77.5,
        "last_observed_volume": 26500.0,
        "max_observed_volume": 54000.0,
        "first_seen_event_time": "2026-03-13T12:55:00+07:00",
        "last_seen_event_time": "2026-03-13T13:00:08+07:00",
        "carry_forward_volume": 18000.0,
        "stale_age_ms": 2437,
        "reappeared_count": 2,
        "executed_volume_at_price": 6200.0,
        "disappeared_volume": 12000.0,
        "trade_explained_ratio": 0.52,
        "cancel_inferred_volume": 5800.0,
        "wall_suspicion_score": 0.68,
        "reconstruction_confidence": 0.74,
        "state": "CARRIED_OUTSIDE_TOP3"
      }
    ],
    "wall_candidates": [
      {
        "price": 77.5,
        "side": "bid",
        "state": "SUSPECTED_FAKE_WALL",
        "wall_suspicion_score": 0.83,
        "reconstruction_confidence": 0.71
      }
    ],
    "reconstruction_quality": {
      "is_reconstructed_input": true,
      "is_full_depth_observed": false
    }
  }
}
```

### 2.7.5. Ý nghĩa output

`deep_book_input_bundle` là **reconstructed input có confidence**, không phải observed raw input. Phase 5 và các phase xử lý tiếp theo phải luôn coi đây là lớp ước lượng, không được xem như full depth thật.

## 2.8. Phase 5 — Layer-2 ready input package

### 2.8.1. Mục tiêu

Đóng gói observed input đã chuẩn hóa và reconstructed input đã gắn confidence thành object đầu vào cuối cùng của PHẦN 2.

### 2.8.2. Input của phase

Input là:

- `validated_input_bundle`
- `aligned_input_frame`
- `deep_book_input_bundle`

### 2.8.3. Xử lý chính

- Tách rõ observed input và reconstructed input.
- Chọn đúng object quan sát được tại `reference_time`.
- Đưa degraded flags, validation report và reconstruction confidence về cùng một `input_quality` block.
- Đảm bảo package cuối cùng đã đủ:
  - daily context
  - intraday cumulative snapshot
  - order book snapshot
  - trade tick windows
  - deep book reconstructed state
  - quality metadata

### 2.8.4. Output của phase

Output là `layer2_input_package`.

```json
{
  "layer2_input_package": {
    "symbol": "FPT",
    "reference_time": "2026-03-13T13:00:10.437000+07:00",
    "daily_input": "normalized_daily_input",
    "intraday_snapshot_input": "aligned intraday snapshot",
    "orderbook_input": "aligned top-3 orderbook snapshot",
    "trade_tick_input": {
      "last_trade": "latest aligned trade tick",
      "trade_windows": {
        "1s": "...",
        "5s": "...",
        "10s": "...",
        "60s": "...",
        "5m": "..."
      }
    },
    "deep_book_input": "deep_book_input_bundle",
    "input_quality": {
      "validation_report": "input_validation_report",
      "degraded_flags": {
        "book_total_depth_unusable": true,
        "trade_side_nullable": true
      },
      "age_metadata": {
        "book_age_ms": 0,
        "tick_age_ms": 20,
        "intraday_snapshot_age_ms": 0
      },
      "reconstruction_confidence_summary": {
        "deep_book_available": true,
        "deep_book_is_estimated": true
      }
    }
  }
}
```

### 2.8.5. Ý nghĩa output

`layer2_input_package` là object đầu vào chuẩn của Layer 2 để chuyển sang **PHẦN 3 — Pipeline xử lý nội bộ**. Từ thời điểm này, các phase phía sau không còn làm việc trực tiếp với file raw mà làm việc với package đã được chuẩn hóa, validate, align và phân lớp observed/reconstructed.

## 2.9. Bảng tóm tắt output của từng phase

| Output object | Được tạo ở phase | Bản chất | Dùng cho phase sau như thế nào |
| --- | --- | --- | --- |
| `raw_source_bundle` | Phase 0 | Observed raw input đã qua session gate | Là nguồn vào để chuẩn hóa schema |
| `normalized_input_bundle` | Phase 1 | Observed input đã đổi sang schema nội bộ | Là nguồn vào cho validation và kiểm tra nhất quán |
| `validated_input_bundle` | Phase 2 | Observed input + validation report + degraded flags | Là nguồn vào cho time alignment và quality propagation |
| `aligned_input_frame` | Phase 3 | Observed input đã căn theo `reference_time` | Là nguồn vào trực tiếp cho market-state/feature pipeline và cho Phase 4 |
| `deep_book_input_bundle` | Phase 4 | Reconstructed internal input có confidence | Bổ sung latent depth, wall memory và fake-wall candidates cho package cuối |
| `layer2_input_package` | Phase 5 | Package cuối cùng gồm observed input + reconstructed input + quality | Là input chuẩn để chuyển sang PHẦN 3 — Pipeline xử lý nội bộ |

PHẦN 3 — Thiết kế các bước xử lý nội bộ của Layer 2
Hãy chia Layer 2 thành các sub-module rõ ràng, ví dụ:
1. schema normalization
2. cleaning & anomaly handling
3. time alignment & synchronization
4. market state construction
5. feature engineering
6. feature quality scoring

Với mỗi sub-module, hãy mô tả:
- input
- xử lý
- output
- ví dụ

PHẦN 4 — Thiết kế output schema của Layer 2
Hãy định nghĩa đầy đủ output schema gồm:

A. market_state
Bao gồm nhưng không giới hạn:
- last_trade_price
- last_trade_volume
- last_trade_side
- cum_volume
- trade_count_intraday
- best_bid
- best_bid_size
- best_ask
- best_ask_size
- mid_price
- spread
- spread_bps
- bid_depth_l1 / l3 / l5
- ask_depth_l1 / l3 / l5
- day_open
- day_high_so_far
- day_low_so_far
- current_price
- vwap_intraday
- intraday_return_from_open
- intraday_pos_in_range
- book_age_ms
- tick_age_ms

B. feature_store
Chia feature thành các nhóm rõ ràng:
1. price return features
2. volatility / range features
3. tick / trade flow features
4. order book features
5. daily technical features
6. daily context features
7. hybrid / composite features

Trong đó cần bao gồm tối thiểu các field như:
- ret_1s, ret_5s, ret_30s, ret_60s
- rv_10s, rv_60s, rv_5m
- trade_count_10s, trade_intensity_10s, volume_sum_10s
- signed_volume_10s, signed_volume_60s
- book_imbalance_l1, book_imbalance_l3, book_imbalance_l5
- depth_ratio_l1, depth_ratio_l3, depth_ratio_l5
- queue_pressure_l1
- ma_5, ma_10, ma_20, ma_50, ma_100, ma_200
- ema_12, ema_20, ema_26
- rsi_14_d
- macd_line_d, macd_signal_d, macd_hist_d
- bollinger_mid_20, bollinger_upper_20, bollinger_lower_20
- atr_14
- volatility_20d
- avg_volume_20d
- price_to_ma20
- price_to_ma200
- ma20_slope_5d
- ma20_ma200_ratio
- gap_pct
- relative_volume_day
- foreign_net_volume_day
- foreign_net_ratio_day
- intraday_pos_in_range
- distance_to_day_high
- distance_to_day_low
- signed_volume_x_imbalance
- price_to_vwap_x_relative_volume

C. feature_quality
Bao gồm tối thiểu:
- tick_coverage_10s
- tick_coverage_60s
- book_coverage_10s
- book_coverage_60s
- daily_history_available_days
- stale_tick_ms
- stale_book_ms
- stale_book_seconds
- trade_side_inferred_ratio
- book_reconstructed_flag
- missing_depth_level_count
- outlier_flag
- spread_invalid_flag
- duplicate_event_flag
- session_valid_flag
- auction_flag
- near_close_flag
- feature_confidence
- data_quality_score

PHẦN 5 — Giải thích ý nghĩa từng field quan trọng
Hãy giải thích ý nghĩa nghiệp vụ và ý nghĩa thị trường của các field chính, đặc biệt:
- best_bid
- best_bid_size
- best_ask
- best_ask_size
- spread
- cum_volume
- vwap_intraday
- ret_5s
- rv_60s
- trade_intensity_10s
- signed_volume_10s
- book_imbalance_l1
- book_imbalance_l3
- spread_bps
- depth_ratio_l3
- foreign_net_ratio_day
- relative_volume_day
- intraday_pos_in_range
- ma_20
- ma_200
- rsi_14_d
- macd_line_d
- macd_signal_d
- macd_hist_d
- feature_confidence

Với mỗi field, hãy mô tả:
- định nghĩa
- công thức gần đúng
- ý nghĩa thị trường
- vai trò với Layer 3

PHẦN 6 — Cho ví dụ dữ liệu mẫu hoàn chỉnh
- Hãy tạo ví dụ input mẫu cho daily, bid/ask, tick.
- Sau đó tạo 1 output row hoàn chỉnh của Layer 2 tại một timestamp cụ thể.
- Output row phải có đủ:
  - symbol
  - timestamp
  - market_state
  - feature_store
  - feature_quality

PHẦN 7 — Chỉ ra những gì nên và không nên làm ở Layer 2
Ví dụ:
NÊN:
- tính MA, EMA, RSI, MACD, ATR
- tính order book imbalance
- tính trade intensity
- tính relative volume
- tính confidence score

KHÔNG NÊN:
- phát sinh rule buy/sell
- phát sinh lệnh giao dịch
- tối ưu portfolio
- tính allocation
- train model dự đoán
- dùng dữ liệu tương lai

PHẦN 8 — Đưa ra cấu trúc triển khai thực tế
Hãy đề xuất:
- class / struct / schema cho output Layer 2
- folder/module structure
- tên module xử lý
- luồng dữ liệu từ input đến output

PHẦN 9 — Trình bày kết quả thật có cấu trúc
Yêu cầu đầu ra phải chia rõ:
1. Tổng quan
2. Input
3. Pipeline xử lý
4. Output schema
5. Giải thích feature
6. Ví dụ dữ liệu
7. Khuyến nghị triển khai   

Viết theo văn phong kỹ thuật, dễ đọc, rõ ràng, thực chiến.
Ưu tiên trình bày bằng markdown, bảng và JSON mẫu.
Không được trả lời chung chung.
Không được bỏ qua các field quan trọng.

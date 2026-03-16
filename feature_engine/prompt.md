Bạn hãy thiết kế và triển khai Layer 3 cho chương trình này.

## Mục tiêu
Layer 3 là process độc lập mới dùng để:
- dựng `market_state`
- tính `feature_store`
- tính `feature_quality`
- đóng gói output cuối cùng cho pipeline phía sau

Layer 3 phải:
- đặt code trong folder `feature_engine`
- đọc input từ `data_processing/data`
- chia phase tương tự Layer 2
- mỗi phase phải ghi output JSON riêng ra file để đối chiếu chéo
- không dùng dữ liệu tương lai

---

## Root code bắt buộc
Tạo cấu trúc code:

- `feature_engine/app_layer3_pipeline.py`
- `feature_engine/layer3/__init__.py`
- `feature_engine/layer3/phase0.py`
- `feature_engine/layer3/phase1.py`
- `feature_engine/layer3/phase2.py`
- `feature_engine/layer3/phase3.py`
- `feature_engine/layer3/phase4.py`
- `feature_engine/layer3/phase5.py`

Output root mặc định:
- `feature_engine/data/{symbol}/...`

Không tạo file log dạng `*_logs`.
Chỉ cần in progress ra stdout giống Layer 2.

---

## CLI bắt buộc
`feature_engine/app_layer3_pipeline.py` phải hỗ trợ:

- `--run-once`
- `--symbols`
- `--interval-seconds`
- `--input-root`
- `--output-root`

Mặc định:
- `interval = 5s`
- `input-root = data_processing/data`
- `output-root = feature_engine/data`

Một vòng `run_once()` phải chạy full:
- phase0
- phase1
- phase2
- phase3
- phase4
- phase5

Mỗi phase phải có dirty detection theo file signature của input phase trước.
Nếu không có thay đổi thì phase đó phải `idle`.

---

## Input bắt buộc từ Layer 2
Layer 3 không đọc trực tiếp từ `data_ingestion`.

Layer 3 phải đọc từ `data_processing/data/{symbol}` các bundle sau:

1. `phase2_input_validation/{year}/{date}.json`
   - object: `validated_input_bundle`

2. `phase3_time_alignment_reference_indexing/{year}/{date}.json`
   - object: `aligned_input_frame`

3. `phase4_book_memory_deep_reconstruction/{year}/{date}.json`
   - object: `deep_book_input_bundle`

Layer 3 phải dùng cả 3 nguồn trên.
Không được giả định rằng Layer 2 đã có phase5.

---

## Timezone và time axis
Quy ước bắt buộc:

- `reference_time_local` dùng timezone `UTC+07:00`
- trục xử lý chính của Layer 3 là `reference_time_local` từ Phase 3
- mọi feature intraday đều phải tính theo từng `reference_time`
- không dùng dữ liệu sau `reference_time`

---

## Granularity bắt buộc
Output chính của Layer 3 phải là **per-frame**, không phải chỉ per-day.

Nghĩa là:
- mỗi ngày có nhiều frame
- mỗi frame gắn với một `reference_time_local`
- `market_state`, `feature_store`, `feature_quality` phải bám theo từng frame

Mỗi output phase phải trace được:
- `symbol`
- `trading_date`
- `frame_no`
- `reference_time_local`
- `reference_time_utc`

---

## Kiến trúc phase bắt buộc

### Phase 0 — Layer2 input intake & dependency check
Output name:
- `layer3_input_bundle`

Mục tiêu:
- đọc 3 bundle từ Layer 2
- check consistency:
  - `symbol`
  - `trading_date`
  - số frame
  - `reference_time`
- gom dependency lại thành input chuẩn cho Layer 3

Output phải giữ rõ:
- daily branch
- aligned observed branch
- deep book branch
- metadata trace-back tới file nguồn

---

### Phase 1 — Market state construction
Output name:
- `market_state_bundle`

Mục tiêu:
- dựng `market_state` cho từng `reference_time`

Mỗi frame phải có tối thiểu:
- `last_trade_price`
- `last_trade_volume`
- `last_trade_side`
- `cum_volume`
- `trade_count_intraday`
- `best_bid`
- `best_bid_size`
- `best_ask`
- `best_ask_size`
- `mid_price`
- `spread`
- `spread_bps`
- `bid_depth_l1`
- `bid_depth_l3`
- `bid_depth_l5`
- `ask_depth_l1`
- `ask_depth_l3`
- `ask_depth_l5`
- `day_open`
- `day_high_so_far`
- `day_low_so_far`
- `current_price`
- `vwap_intraday`
- `intraday_return_from_open`
- `intraday_pos_in_range`
- `book_age_ms`
- `tick_age_ms`

Quy ước:
- depth `l1` lấy top 1
- depth `l3` lấy tổng top 3 observed
- depth `l5` nếu không đủ observed depth thật thì dùng observed top3 + reconstructed memory từ Phase 4 nếu có; đồng thời phải phản ánh điều này trong quality flag

---

### Phase 2 — Base intraday feature engineering
Output name:
- `base_feature_bundle`

Mục tiêu:
- tính feature intraday cơ sở cho từng frame

Feature tối thiểu:
- `ret_1s`
- `ret_5s`
- `ret_30s`
- `ret_60s`
- `rv_10s`
- `rv_60s`
- `rv_5m`
- `trade_count_10s`
- `trade_intensity_10s`
- `volume_sum_10s`
- `signed_volume_10s`
- `signed_volume_60s`
- `book_imbalance_l1`
- `book_imbalance_l3`
- `book_imbalance_l5`
- `depth_ratio_l1`
- `depth_ratio_l3`
- `depth_ratio_l5`
- `queue_pressure_l1`

Quy ước:
- các feature intraday phải tính từ Phase 3
- nếu cần trade windows thì dùng `trade_window_index_ranges`
- không dùng data tương lai
- nếu không đủ data thì field vẫn phải có nhưng để `null` và phản ánh ở quality phase sau

---

### Phase 3 — Daily technical + daily context + hybrid features
Output name:
- `feature_store_bundle`

Mục tiêu:
- tính các feature daily technical, daily context, hybrid/composite

Feature tối thiểu:
- `ma_5`
- `ma_10`
- `ma_20`
- `ma_50`
- `ma_100`
- `ma_200`
- `ema_12`
- `ema_20`
- `ema_26`
- `rsi_14_d`
- `macd_line_d`
- `macd_signal_d`
- `macd_hist_d`
- `bollinger_mid_20`
- `bollinger_upper_20`
- `bollinger_lower_20`
- `atr_14`
- `volatility_20d`
- `avg_volume_20d`
- `price_to_ma20`
- `price_to_ma200`
- `ma20_slope_5d`
- `ma20_ma200_ratio`
- `gap_pct`
- `relative_volume_day`
- `foreign_net_volume_day`
- `foreign_net_ratio_day`
- `intraday_pos_in_range`
- `distance_to_day_high`
- `distance_to_day_low`
- `signed_volume_x_imbalance`
- `price_to_vwap_x_relative_volume`

### Daily lookback rule bắt buộc
Các feature daily như `MA/EMA/RSI/MACD/Bollinger/ATR/volatility_20d/avg_volume_20d` không thể tính chỉ từ 1 ngày.

Vì vậy Layer 3 được phép đọc thêm **daily history** của cùng symbol từ:
- ưu tiên: `data_ingestion/data/{symbol}/historical_price`
- hoặc một daily source ổn định tương đương trong `data_processing/data`

Phải ghi rõ trong code:
- source daily history dùng để tính technical features là gì
- số ngày lookback tối thiểu thực tế lấy được là bao nhiêu

---

### Phase 4 — Feature quality scoring
Output name:
- `feature_quality_bundle`

Mục tiêu:
- đánh giá chất lượng dữ liệu đầu vào và chất lượng feature tại từng frame

Field tối thiểu:
- `tick_coverage_10s`
- `tick_coverage_60s`
- `book_coverage_10s`
- `book_coverage_60s`
- `daily_history_available_days`
- `stale_tick_ms`
- `stale_book_ms`
- `stale_book_seconds`
- `trade_side_inferred_ratio`
- `book_reconstructed_flag`
- `missing_depth_level_count`
- `outlier_flag`
- `spread_invalid_flag`
- `duplicate_event_flag`
- `session_valid_flag`
- `auction_flag`
- `near_close_flag`
- `feature_confidence`
- `data_quality_score`

Quy ước:
- `book_reconstructed_flag = true` nếu feature/frame phải dùng deep-book reconstructed data
- `feature_confidence` và `data_quality_score` phải là score `0..1`
- cần tách rõ field nào là raw check, field nào là aggregated score

---

### Phase 5 — Final Layer3 output package
Output name:
- `layer3_output_package`

Mục tiêu:
- đóng gói output cuối cùng cho mỗi frame

Mỗi frame output phải gồm:
- `market_state`
- `feature_store`
- `feature_quality`
- trace-back metadata:
  - `symbol`
  - `trading_date`
  - `frame_no`
  - `reference_time_local`
  - `reference_time_utc`
  - `source_bundle_paths`

---

## Định nghĩa công thức / quy ước bắt buộc
Khi triển khai, phải ghi rõ trong code hoặc metadata output các quy ước sau:

- `spread = best_ask - best_bid`
- `spread_bps = spread / mid_price * 10000` nếu `mid_price > 0`
- `mid_price = (best_bid + best_ask) / 2` nếu đủ 2 phía
- `vwap_intraday = cum_trade_value / cum_volume` nếu `cum_volume > 0`
- `intraday_return_from_open = current_price / day_open - 1`
- `intraday_pos_in_range = (current_price - day_low_so_far) / (day_high_so_far - day_low_so_far)` nếu range > 0
- `book_imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)` nếu mẫu số > 0
- `depth_ratio = bid_depth / ask_depth` nếu `ask_depth > 0`
- `queue_pressure_l1` phải ghi rõ công thức trong code
- `signed_volume` dùng `trade.side`; nếu `side = null` thì phải có quy tắc xử lý rõ ràng
- mọi return / volatility feature phải dùng dữ liệu không vượt `reference_time`

---

## Output file structure bắt buộc
Mỗi phase phải ghi file theo cấu trúc:

- `feature_engine/data/{symbol}/phase0_layer2_input_intake/{year}/{date}.json`
- `feature_engine/data/{symbol}/phase1_market_state_construction/{year}/{date}.json`
- `feature_engine/data/{symbol}/phase2_base_feature_engineering/{year}/{date}.json`
- `feature_engine/data/{symbol}/phase3_feature_store/{year}/{date}.json`
- `feature_engine/data/{symbol}/phase4_feature_quality/{year}/{date}.json`
- `feature_engine/data/{symbol}/phase5_layer3_output_package/{year}/{date}.json`

Mỗi file phải đủ chi tiết để đối chiếu chéo với phase trước.
Không chỉ giữ summary; cần giữ dữ liệu cần thiết để trace ngược.

---

## Yêu cầu về traceability
AI phải đảm bảo:

- từ `feature_store_bundle` có thể trace ngược về `market_state_bundle`
- từ `market_state_bundle` có thể trace ngược về `aligned_input_frame`
- từ `deep-book` related feature có thể trace ngược về `deep_book_input_bundle`
- từ technical daily feature có thể trace ngược về daily history source

---

## Yêu cầu triển khai
- Hãy viết code chạy được.
- Hãy dùng lại style tổ chức giống Layer 2.
- Hãy smoke-test bằng dữ liệu thật ít nhất với:
  - `python feature_engine/app_layer3_pipeline.py --run-once --symbols FPT`
- Hãy báo rõ:
  - phase nào built
  - phase nào idle
  - output file nào được tạo ra

## Lưu ý rất quan trọng
- Layer 3 phải dùng Phase 3 của Layer 2 làm nguồn observed intraday chính.
- Layer 3 phải dùng Phase 4 của Layer 2 như nguồn reconstructed/deep-book phụ trợ.
- Không được làm mất dữ liệu tick/orderbook quan trọng đã có ở Layer 2.
- Không được rút gọn thành chỉ còn summary cuối ngày.
- Tất cả market state / feature / quality phải bám theo từng `reference_time`.

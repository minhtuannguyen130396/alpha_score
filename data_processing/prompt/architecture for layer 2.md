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

PHẦN 2 — Thiết kế input schema của Layer 2
- Mô tả chi tiết input schema cho:
  1. daily data
  2. bid/ask log
  3. tick data
- Cho ví dụ JSON mẫu cho từng loại input.
- Nêu rõ field nào bắt buộc, field nào tùy chọn.

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
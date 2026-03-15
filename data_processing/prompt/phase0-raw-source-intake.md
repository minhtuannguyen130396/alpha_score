2.3. Phase 0 — Raw source intake
2.3.1. Mục tiêu

Phase này chỉ làm nhiệm vụ nhận dữ liệu gốc từ Layer 1 hoặc storage.
Ở đây chưa làm feature, chưa suy luận, chưa sửa dữ liệu.

2.3.2. Input của phase

Phase này nhận 4 raw source group thực tế:

historical_price

updatelastprices

updateorderbooks

updatetrades

2.3.3. Cấu trúc input theo từng group
G0.1 — Raw daily source

Nguồn: historical_price
Field daily quan trọng gồm:

date

symbol

priceOpen

priceHigh

priceLow

priceClose

priceBasic

priceAverage

totalVolume

dealVolume

putthroughVolume

totalValue

putthroughValue

buyForeignQuantity

buyForeignValue

sellForeignQuantity

sellForeignValue

buyCount

buyQuantity

sellCount

sellQuantity

adjRatio

currentForeignRoom

propTradingNetDealValue

propTradingNetPTValue

propTradingNetValue

unit

G0.2 — Raw intraday cumulative source

Nguồn: updatelastprices
Field quan trọng gồm:

ts

command

symbol

event_time

last_price

last_volume

deal_volume

deal_value

matched_price

active_buy_volume

active_sell_volume

total_buy_sale_volume

G0.3 — Raw order book source

Nguồn: updateorderbooks
Field quan trọng gồm:

ts

command

symbol

event_time

total_bid_volume

total_ask_volume

bid_levels

ask_levels

bid_levels[].price

bid_levels[].volume

ask_levels[].price

ask_levels[].volume

G0.4 — Raw trade tick source

Nguồn: updatetrades
Field quan trọng gồm:

ts

command

symbol

trade_id

event_time

price

volume

side

trading_date

2.3.4. Output của phase
raw_source_bundle

Đây là object gốc của Layer 2, gồm 4 nhóm:

{
  "raw_daily_records": "...",
  "raw_lastprice_records": "...",
  "raw_orderbook_records": "...",
  "raw_trade_records": "..."
}
hãy đưa ra file output đại diện cho phase0 này dạng text
Thiết kế kiểu độc lập dễ dàng xóa đi khi không cần 
2.3.5. Ý nghĩa output

raw_source_bundle là tập raw input chưa chuẩn hóa, dùng làm đầu vào cho Phase 1.
# Data Mapping Guide

This document summarizes the meaning of fields in four data groups:

- `historical_price`
- `updatelastprices`
- `updateorderbooks`
- `updatetrades`

Goals:
- help read the schema quickly
- help map fields into a normalized schema
- help distinguish fields that are easy to confuse

## 1. historical_price

Daily trading data. Each file usually contains one month of data and is stored as a `JSON array`.

| Field | Short explanation |
|---|---|
| `date` | Trading date of the record |
| `symbol` | Stock symbol |
| `priceHigh` | Highest price of the day |
| `priceLow` | Lowest price of the day |
| `priceOpen` | Opening price |
| `priceAverage` | Average traded price during the day |
| `priceClose` | Closing price |
| `priceBasic` | Reference price |
| `totalVolume` | Total trading volume |
| `dealVolume` | Matched-order volume |
| `putthroughVolume` | Put-through volume |
| `totalValue` | Total trading value |
| `putthroughValue` | Put-through trading value |
| `buyForeignQuantity` | Foreign investor buy volume |
| `buyForeignValue` | Foreign investor buy value |
| `sellForeignQuantity` | Foreign investor sell volume |
| `sellForeignValue` | Foreign investor sell value |
| `buyCount` | Number of buy orders |
| `buyQuantity` | Total buy order quantity |
| `sellCount` | Number of sell orders |
| `sellQuantity` | Total sell order quantity |
| `adjRatio` | Historical price adjustment ratio |
| `currentForeignRoom` | Remaining foreign ownership room |
| `propTradingNetDealValue` | Proprietary net value from matched orders |
| `propTradingNetPTValue` | Proprietary net value from put-through trades |
| `propTradingNetValue` | Total proprietary net value |
| `unit` | Value unit, usually `1000` |

Notes:
- In the current dataset: `propTradingNetValue = propTradingNetDealValue + propTradingNetPTValue`

## 2. updatelastprices

Intraday event log data. Each file is usually stored as `JSON Lines`.

| Field | Short explanation |
|---|---|
| `ts` | Time when the system wrote the record |\   
| `command` | Message type, usually `UpdateLastPrices` |
| `symbol` | Stock symbol |
| `event_time` | Time when the market event occurred |
| `last_price` | Most recent matched price |
| `last_volume` | Volume of the most recent match |
| `total_volume` | Cumulative matched volume |
| `total_value` | Cumulative matched value |
| `matched_price` | Extra matched-price field from the source feed. Downstream pipeline no longer consumes this field because it is inconsistent in current raw data. |
| `active_buy_volume` | Cumulative active buy volume |
| `active_sell_volume` | Cumulative active sell volume |
| `total_buy_sale_volume` | Total buy/sell volume, currently equal to `total_volume` in the checked data |

Notes:
- In the current dataset:
- `matched_price` is not reliable enough to be treated as equivalent to `last_price`
- `total_buy_sale_volume = total_volume`
- `active_sell_volume = total_buy_sale_volume - active_buy_volume`

## 3. updateorderbooks

Intraday order book snapshots. Each file is usually stored as `JSON Lines`.

| Field | Short explanation |
|---|---|
| `ts` | Time when the system wrote the record |
| `command` | Message type, usually `UpdateOrderBooks` |
| `symbol` | Stock symbol |
| `event_time` | Time when the order book snapshot occurred |
| `total_bid_volume` | Total bid-side volume |
| `total_ask_volume` | Total ask-side volume |
| `bid_levels` | List of bid price levels |
| `ask_levels` | List of ask price levels |

Structure of each item inside `bid_levels` and `ask_levels`:

| Subfield | Short explanation |
|---|---|
| `price` | Quoted price level |
| `volume` | Queued volume at that price level |

Notes:
- `bid_levels`: best buy levels
- `ask_levels`: best sell levels

## 4. updatetrades

Matched trade event data. Each file is usually stored as `JSON Lines`.

| Field | Short explanation |
|---|---|
| `ts` | Time when the system wrote the record |
| `command` | Message type, usually `UpdateTrades` |
| `symbol` | Stock symbol |
| `trade_id` | Trade identifier |
| `event_time` | Time when the trade occurred |
| `price` | Trade price |
| `volume` | Trade volume |
| `side` | Trade side, usually `B` or `S` |
| `trading_date` | Trading date |

## 5. Quick mapping hints

Some field pairs are easy to confuse during mapping:

| Field A | Field B | Assessment |
|---|---|---|
| `historical_price.totalVolume` | `updatelastprices.total_volume` | Same general meaning, but different level: end-of-day vs intraday snapshot |
| `historical_price.totalValue` | `updatelastprices.total_value` | Same general meaning, but different level: end-of-day vs intraday snapshot |
| `updatetrades.price` | `updatelastprices.last_price` | Similar meaning, but `price` is per trade while `last_price` is the latest snapshot price |
| `updatetrades.volume` | `updatelastprices.last_volume` | Similar meaning, but `volume` is per trade while `last_volume` is the latest matched volume |
| `updatelastprices.total_volume` | `updatelastprices.total_buy_sale_volume` | Equal in the current dataset |

## 6. Processing notes

- `ts` is the record write time, not the original market event time.
- `event_time` is the actual market event time.
- `historical_price` is end-of-day data.
- `update*` folders contain intraday event logs.
- Do not merge similarly named fields blindly if they belong to different time granularities.

import json
from datetime import datetime
from pathlib import Path


BASE_DIR = Path("data_ingestion/data/FPT")
TARGET_DATE = "2026-03-13"
OUTPUT_PATH = BASE_DIR / "normalized_daily_trial" / f"{TARGET_DATE}.json"


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_ndjson(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def parse_dt(value: str | None):
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def latest_record(records):
    def sort_key(record):
        return (
            parse_dt(record.get("event_time")) or datetime.min,
            parse_dt(record.get("ts")) or datetime.min,
        )

    return sorted(records, key=sort_key)[-1]


def get_historical_day():
    monthly_file = BASE_DIR / "historical_price" / "2026-03-01.json"
    for record in read_json(monthly_file):
        if str(record.get("date", ""))[:10] == TARGET_DATE:
            return record
    raise ValueError(f"Historical record not found for {TARGET_DATE}")


def get_latest_intraday(folder_name: str):
    path = BASE_DIR / folder_name / "2026" / f"{TARGET_DATE}.json"
    rows = read_ndjson(path)
    rows = [row for row in rows if str(row.get("symbol")) == "FPT"]
    if not rows:
        raise ValueError(f"No rows found in {path}")
    return latest_record(rows)


def build_output():
    historical = get_historical_day()
    reference_price = get_latest_intraday("updaterefprices")
    last_price = get_latest_intraday("updatelastprices")
    foreigner_stats = get_latest_intraday("updateforeignerstats")

    return {
        "symbol": "FPT",
        "date": TARGET_DATE,
        "common": {
            "symbol": "FPT",
            "time": reference_price.get("event_time"),
            "open": last_price.get("last_price"),
            "close": reference_price.get("open_price"),
            "high": reference_price.get("high_price"),
            "low": reference_price.get("low_price"),
            "reference": historical.get("priceBasic"),
            "average": historical.get("priceAverage"),
            "total_volume": historical.get("totalVolume"),
            "deal_volume": historical.get("dealVolume"),
            "putthrough_volume": historical.get("putthroughVolume"),
            "total_value": historical.get("totalValue"),
            "putthrough_value": historical.get("putthroughValue"),
            "buy_foreign_quantity": historical.get("buyForeignQuantity"),
            "buy_foreign_value": historical.get("buyForeignValue"),
            "sell_foreign_quantity": historical.get("sellForeignQuantity"),
            "sell_foreign_value": historical.get("sellForeignValue"),
            "buy_count": historical.get("buyCount"),
            "buy_quantity": historical.get("buyQuantity"),
            "sell_count": historical.get("sellCount"),
            "sell_quantity": historical.get("sellQuantity"),
            "adj_ratio": historical.get("adjRatio"),
            "current_foreign_room": historical.get("currentForeignRoom"),
            "prop_trading_net_deal_value": historical.get("propTradingNetDealValue"),
            "prop_trading_net_pt_value": historical.get("propTradingNetPTValue"),
            "prop_trading_net_value": historical.get("propTradingNetValue"),
            "unit": historical.get("unit"),
        },
        "reference_price": {
            "snapshot_time": reference_price.get("event_time"),
            "ingested_at": reference_price.get("ts"),
            "trading_date": str(reference_price.get("trading_date", ""))[:10],
            "reference_price": reference_price.get("reference_price"),
            "ceiling_price": reference_price.get("ceiling_price"),
            "floor_price": reference_price.get("floor_price"),
            "open_price": reference_price.get("open_price"),
            "high_price": reference_price.get("high_price"),
            "low_price": reference_price.get("low_price"),
            "market_status": reference_price.get("market_status"),
        },
        "last_price": {
            "snapshot_time": last_price.get("event_time"),
            "ingested_at": last_price.get("ts"),
            "last_price": last_price.get("last_price"),
            "last_volume": last_price.get("last_volume"),
            "total_volume": last_price.get("total_volume"),
            "total_value": last_price.get("total_value"),
            "active_buy_volume": last_price.get("active_buy_volume"),
            "active_sell_volume": last_price.get("active_sell_volume"),
            "total_buy_sale_volume": last_price.get("total_buy_sale_volume"),
        },
        "foreigner_stats": {
            "snapshot_time": foreigner_stats.get("event_time"),
            "ingested_at": foreigner_stats.get("ts"),
            "net_volume": foreigner_stats.get("net_volume"),
            "buy_volume": foreigner_stats.get("buy_volume"),
            "sell_volume": foreigner_stats.get("sell_volume"),
            "buy_value": foreigner_stats.get("buy_value"),
            "sell_value": foreigner_stats.get("sell_value"),
        },
    }


def main():
    output = build_output()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

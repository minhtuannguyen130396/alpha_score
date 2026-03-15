import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Normalize monthly historical and intraday update JSON files into one JSON file per trading day."
    )
    parser.add_argument(
        "--symbol-dir",
        default="data_ingestion/data/FPT",
        help="Path to the symbol directory containing historical_price and update* folders.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory. Defaults to <symbol-dir>/normalized_daily.",
    )
    return parser.parse_args()


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_ndjson(path: Path):
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def to_date_str(value):
    if not value:
        return None
    if isinstance(value, str):
        return value[:10]
    return str(value)[:10]


def parse_dt(value):
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def choose_latest(records):
    if not records:
        return None

    def sort_key(record):
        event_time = parse_dt(record.get("event_time")) or datetime.min
        ts = parse_dt(record.get("ts")) or datetime.min
        return (event_time, ts)

    return sorted(records, key=sort_key)[-1]


def normalize_historical_record(record, source_file):
    return {
        "source_file": str(source_file),
        "symbol": record.get("symbol"),
        "date": to_date_str(record.get("date")),
        "open": record.get("priceOpen"),
        "high": record.get("priceHigh"),
        "low": record.get("priceLow"),
        "close": record.get("priceClose"),
        "reference": record.get("priceBasic"),
        "average": record.get("priceAverage"),
        "total_volume": record.get("totalVolume"),
        "deal_volume": record.get("dealVolume"),
        "putthrough_volume": record.get("putthroughVolume"),
        "total_value": record.get("totalValue"),
        "putthrough_value": record.get("putthroughValue"),
        "buy_foreign_quantity": record.get("buyForeignQuantity"),
        "buy_foreign_value": record.get("buyForeignValue"),
        "sell_foreign_quantity": record.get("sellForeignQuantity"),
        "sell_foreign_value": record.get("sellForeignValue"),
        "buy_count": record.get("buyCount"),
        "buy_quantity": record.get("buyQuantity"),
        "sell_count": record.get("sellCount"),
        "sell_quantity": record.get("sellQuantity"),
        "adj_ratio": record.get("adjRatio"),
        "current_foreign_room": record.get("currentForeignRoom"),
        "prop_trading_net_deal_value": record.get("propTradingNetDealValue"),
        "prop_trading_net_pt_value": record.get("propTradingNetPTValue"),
        "prop_trading_net_value": record.get("propTradingNetValue"),
        "unit": record.get("unit"),
    }


def normalize_last_price_record(record, source_file):
    return {
        "source_file": str(source_file),
        "snapshot_time": record.get("event_time"),
        "ingested_at": record.get("ts"),
        "last_price": record.get("last_price"),
        "last_volume": record.get("last_volume"),
        "total_volume": record.get("total_volume"),
        "total_value": record.get("total_value"),
        "matched_price": record.get("matched_price"),
        "active_buy_volume": record.get("active_buy_volume"),
        "active_sell_volume": record.get("active_sell_volume"),
        "total_buy_sale_volume": record.get("total_buy_sale_volume"),
    }


def normalize_ref_price_record(record, source_file):
    return {
        "source_file": str(source_file),
        "snapshot_time": record.get("event_time"),
        "ingested_at": record.get("ts"),
        "trading_date": to_date_str(record.get("trading_date")),
        "reference_price": record.get("reference_price"),
        "ceiling_price": record.get("ceiling_price"),
        "floor_price": record.get("floor_price"),
        "open_price": record.get("open_price"),
        "high_price": record.get("high_price"),
        "low_price": record.get("low_price"),
        "market_status": record.get("market_status"),
    }


def normalize_foreigner_record(record, source_file):
    return {
        "source_file": str(source_file),
        "snapshot_time": record.get("event_time"),
        "ingested_at": record.get("ts"),
        "net_volume": record.get("net_volume"),
        "buy_volume": record.get("buy_volume"),
        "sell_volume": record.get("sell_volume"),
        "buy_value": record.get("buy_value"),
        "sell_value": record.get("sell_value"),
    }


def build_daily_dataset(symbol_dir: Path):
    daily = defaultdict(
        lambda: {
            "symbol": symbol_dir.name,
            "date": None,
            "historical": None,
            "reference_price": None,
            "last_price": None,
            "foreigner_stats": None,
        }
    )

    historical_dir = symbol_dir / "historical_price"
    for path in sorted(historical_dir.glob("*.json")):
        for record in read_json(path):
            date_str = to_date_str(record.get("date"))
            if not date_str:
                continue
            item = daily[date_str]
            item["date"] = date_str
            item["symbol"] = record.get("symbol") or symbol_dir.name
            item["historical"] = normalize_historical_record(record, path)

    intraday_specs = [
        ("updaterefprices", "reference_price", normalize_ref_price_record),
        ("updatelastprices", "last_price", normalize_last_price_record),
        ("updateforeignerstats", "foreigner_stats", normalize_foreigner_record),
    ]

    for folder_name, output_key, normalizer in intraday_specs:
        folder = symbol_dir / folder_name
        for path in sorted(folder.glob("*/*.json")):
            grouped = defaultdict(list)
            for record in read_ndjson(path):
                date_str = to_date_str(record.get("trading_date")) or to_date_str(record.get("event_time")) or to_date_str(record.get("ts"))
                if not date_str:
                    continue
                grouped[date_str].append(record)

            for date_str, records in grouped.items():
                latest = choose_latest(records)
                if latest is None:
                    continue
                item = daily[date_str]
                item["date"] = date_str
                item["symbol"] = latest.get("symbol") or item["symbol"] or symbol_dir.name
                item[output_key] = normalizer(latest, path)

    return dict(sorted(daily.items()))


def write_daily_files(daily_data, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    for date_str, record in daily_data.items():
        output_path = output_dir / f"{date_str}.json"
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)


def main():
    args = parse_args()
    symbol_dir = Path(args.symbol_dir)
    output_dir = Path(args.output_dir) if args.output_dir else symbol_dir / "normalized_daily"

    daily_data = build_daily_dataset(symbol_dir)
    write_daily_files(daily_data, output_dir)
    print(f"Normalized {len(daily_data)} daily files into {output_dir}")


if __name__ == "__main__":
    main()

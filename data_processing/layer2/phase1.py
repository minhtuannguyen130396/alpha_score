from __future__ import annotations

import json
import shutil
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from layer2.phase0 import (
    BUNDLE_FOLDER as PHASE0_BUNDLE_FOLDER,
    FileSignature,
    LOCAL_TRADING_TZ,
    coerce_date_text,
    default_output_root,
    parse_event_time_utc,
    sanitize_symbol,
    utc_now_iso,
)


INPUT_FOLDER = PHASE0_BUNDLE_FOLDER
BUNDLE_FOLDER = "phase1_schema_normalization"
LOG_FOLDER = "phase1_schema_normalization_logs"


@dataclass
class CachedBundleFile:
    symbol: str
    trading_date: str
    path: Path
    signature: FileSignature
    payload: dict[str, Any]


def load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def parse_timestamp(value: Any, default_tz: timezone | None = None) -> tuple[datetime | None, bool]:
    if value is None:
        return None, False

    try:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value).strip()
            if not text:
                return None, False
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None, False

    assumed_timezone = False
    if parsed.tzinfo is None and default_tz is not None:
        parsed = parsed.replace(tzinfo=default_tz)
        assumed_timezone = True
    return parsed, assumed_timezone


def iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_symbol(raw_symbol: Any, fallback_symbol: str) -> str:
    if raw_symbol is None:
        return fallback_symbol
    return sanitize_symbol(str(raw_symbol).upper())


def normalize_side(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def build_time_fields(raw_record: dict[str, Any]) -> dict[str, Any]:
    event_time_utc = parse_event_time_utc(raw_record.get("event_time"))
    event_time_local = (
        event_time_utc.astimezone(LOCAL_TRADING_TZ) if event_time_utc is not None else None
    )

    ingested_at, assumed_local_timezone = parse_timestamp(
        raw_record.get("ts"),
        default_tz=LOCAL_TRADING_TZ,
    )
    ingested_at_utc = ingested_at.astimezone(timezone.utc) if ingested_at is not None else None
    ingested_at_local = (
        ingested_at.astimezone(LOCAL_TRADING_TZ) if ingested_at is not None else None
    )

    timezone_source = None
    if ingested_at is not None:
        timezone_source = "assumed_utc_plus_7_for_naive_ts" if assumed_local_timezone else "embedded_timezone"

    return {
        "event_time_utc": iso_or_none(event_time_utc),
        "event_time_local": iso_or_none(event_time_local),
        "ingested_at_raw": raw_record.get("ts"),
        "ingested_at_utc": iso_or_none(ingested_at_utc),
        "ingested_at_local": iso_or_none(ingested_at_local),
        "ingested_at_timezone_source": timezone_source,
    }


def normalize_levels(levels: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if not isinstance(levels, list):
        return normalized

    for index, level in enumerate(levels, start=1):
        if not isinstance(level, dict):
            continue
        normalized.append(
            {
                "level_no": index,
                "price": to_float(level.get("price")),
                "volume": to_float(level.get("volume")),
            }
        )

    return normalized


def record_sort_key(record: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(record.get("event_time_utc") or ""),
        str(record.get("ingested_at_utc") or record.get("ingested_at_local") or ""),
        int(record.get("source_sequence") or 0),
    )


def output_bundle_path(output_root: Path, symbol: str, trading_date: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / BUNDLE_FOLDER / trading_date[:4] / f"{trading_date}.json"


def output_log_path(output_root: Path, symbol: str, trading_date: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / LOG_FOLDER / trading_date[:4] / f"{trading_date}.jsonl"


class Phase1SchemaNormalizationProcess:
    def __init__(
        self,
        output_root: Path | None = None,
        symbols: set[str] | None = None,
    ) -> None:
        self.output_root = Path(output_root) if output_root else default_output_root()
        self.symbols = {symbol.upper() for symbol in symbols} if symbols else None
        self._input_cache: dict[str, CachedBundleFile] = {}
        self._first_scan = True

    def run_forever(self, interval_seconds: float = 5.0) -> None:
        interval_seconds = max(float(interval_seconds), 0.1)
        self._stdout(
            f"starting phase1 schema normalization: output={self.output_root} interval={interval_seconds}s"
        )
        while True:
            self.run_once()
            time.sleep(interval_seconds)

    def run_once(self) -> dict[str, int]:
        self._prune_logs()
        dirty_items = self._refresh_input_cache()
        built_count = 0
        removed_count = 0
        failed_count = 0

        for symbol, trading_date in sorted(dirty_items):
            try:
                action = self._materialize_bundle(symbol, trading_date)
                if action == "built":
                    built_count += 1
                elif action == "removed":
                    removed_count += 1
            except Exception as exc:
                failed_count += 1
                self._log_event(
                    symbol,
                    trading_date,
                    {
                        "ts": utc_now_iso(),
                        "level": "ERROR",
                        "status": "failed",
                        "symbol": symbol,
                        "trading_date": trading_date,
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    },
                )
                self._stdout(f"failed normalization bundle for {symbol} {trading_date}: {exc}")

        if not dirty_items:
            self._stdout("phase1 cycle complete: idle")
        else:
            self._stdout(
                f"phase1 cycle complete: dirty={len(dirty_items)} built={built_count} removed={removed_count} failed={failed_count}"
            )

        return {
            "dirty": len(dirty_items),
            "built": built_count,
            "removed": removed_count,
            "failed": failed_count,
        }

    def _stdout(self, message: str) -> None:
        print(f"[{utc_now_iso()}] {message}")

    def _prune_logs(self) -> None:
        if not self.output_root.exists():
            return

        for symbol_dir in sorted(path for path in self.output_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if self.symbols and symbol not in self.symbols:
                continue

            log_dir = symbol_dir / LOG_FOLDER
            if log_dir.exists():
                shutil.rmtree(log_dir)

    def _refresh_input_cache(self) -> set[tuple[str, str]]:
        dirty_items: set[tuple[str, str]] = set()
        discovered_paths: set[str] = set()

        if not self.output_root.exists():
            self._stdout(f"output root not found: {self.output_root}")
            return dirty_items

        for symbol_dir in sorted(path for path in self.output_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if self.symbols and symbol not in self.symbols:
                continue

            input_dir = symbol_dir / INPUT_FOLDER
            if not input_dir.exists():
                continue

            for path in sorted(input_dir.rglob("*.json")):
                cache_key = str(path.resolve())
                discovered_paths.add(cache_key)

                stat = path.stat()
                signature = FileSignature(size=stat.st_size, mtime_ns=stat.st_mtime_ns)
                cached = self._input_cache.get(cache_key)
                if cached and cached.signature == signature:
                    continue

                previous_date = cached.trading_date if cached else None
                payload = load_json_object(path)
                symbol_value = normalize_symbol(payload.get("symbol"), symbol)
                trading_date = coerce_date_text(payload.get("trading_date")) or coerce_date_text(path.stem)
                if not trading_date:
                    raise ValueError(f"missing trading_date in {path}")

                self._input_cache[cache_key] = CachedBundleFile(
                    symbol=symbol_value,
                    trading_date=trading_date,
                    path=path,
                    signature=signature,
                    payload=payload,
                )

                dirty_items.add((symbol_value, trading_date))
                if previous_date and previous_date != trading_date:
                    dirty_items.add((symbol_value, previous_date))

        removed_paths = set(self._input_cache) - discovered_paths
        for cache_key in removed_paths:
            cached = self._input_cache.pop(cache_key)
            dirty_items.add((cached.symbol, cached.trading_date))

        if self._first_scan:
            for cached in self._input_cache.values():
                dirty_items.add((cached.symbol, cached.trading_date))
            self._first_scan = False

        return dirty_items

    def _find_source_bundle(self, symbol: str, trading_date: str) -> CachedBundleFile | None:
        matches = [
            cached
            for cached in self._input_cache.values()
            if cached.symbol == symbol and cached.trading_date == trading_date
        ]
        if not matches:
            return None
        return sorted(matches, key=lambda item: item.path.as_posix())[-1]

    def _materialize_bundle(self, symbol: str, trading_date: str) -> str:
        bundle = self._build_bundle(symbol, trading_date)
        bundle_path = output_bundle_path(self.output_root, symbol, trading_date)

        if bundle is None:
            if bundle_path.exists():
                bundle_path.unlink()
                self._log_event(
                    symbol,
                    trading_date,
                    {
                        "ts": utc_now_iso(),
                        "level": "INFO",
                        "status": "removed",
                        "symbol": symbol,
                        "trading_date": trading_date,
                        "output_path": str(bundle_path),
                    },
                )
                self._stdout(f"removed stale phase1 bundle: {symbol} {trading_date}")
                return "removed"
            return "noop"

        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = bundle_path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(bundle, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(bundle_path)

        self._log_event(
            symbol,
            trading_date,
            {
                "ts": utc_now_iso(),
                "level": "INFO",
                "status": "built",
                "symbol": symbol,
                "trading_date": trading_date,
                "output_path": str(bundle_path),
                "normalized_record_counts": bundle["normalized_record_counts"],
                "source_bundle_path": bundle["source_bundle_path"],
            },
        )
        self._stdout(
            f"built phase1 bundle: {symbol} {trading_date} counts={bundle['normalized_record_counts']}"
        )
        return "built"

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        source_bundle = self._find_source_bundle(symbol, trading_date)
        if source_bundle is None:
            return None

        payload = source_bundle.payload
        raw_daily_records = payload.get("raw_daily_records") or []
        raw_lastprice_records = payload.get("raw_lastprice_records") or []
        raw_orderbook_records = payload.get("raw_orderbook_records") or []
        raw_trade_records = payload.get("raw_trade_records") or []

        normalized_daily_candidates = [
            self._normalize_daily_record(raw_record, symbol, trading_date)
            for raw_record in raw_daily_records
            if isinstance(raw_record, dict)
        ]
        normalized_daily_input = normalized_daily_candidates[-1] if normalized_daily_candidates else None

        normalized_intraday_snapshot_input = [
            self._normalize_intraday_snapshot_record(raw_record, symbol, trading_date, index)
            for index, raw_record in enumerate(raw_lastprice_records, start=1)
            if isinstance(raw_record, dict)
        ]
        normalized_orderbook_input = [
            self._normalize_orderbook_record(raw_record, symbol, trading_date, index)
            for index, raw_record in enumerate(raw_orderbook_records, start=1)
            if isinstance(raw_record, dict)
        ]
        normalized_trade_tick_input = [
            self._normalize_trade_record(raw_record, symbol, trading_date, index)
            for index, raw_record in enumerate(raw_trade_records, start=1)
            if isinstance(raw_record, dict)
        ]

        normalized_intraday_snapshot_input.sort(key=record_sort_key)
        normalized_orderbook_input.sort(key=record_sort_key)
        normalized_trade_tick_input.sort(key=record_sort_key)

        return {
            "bundle_type": "normalized_input_bundle",
            "phase": "phase1_schema_normalization",
            "symbol": symbol,
            "trading_date": trading_date,
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "source_group_status": payload.get("source_group_status"),
            "source_record_counts": payload.get("record_counts"),
            "session_filter_local": payload.get("session_filter_local"),
            "normalization_metadata": {
                "daily_record_candidates": len(normalized_daily_candidates),
                "selected_daily_record_position": len(normalized_daily_candidates)
                if normalized_daily_candidates
                else None,
                "naive_ts_assumed_timezone": "UTC+07:00",
            },
            "normalized_record_counts": {
                "daily_records": 1 if normalized_daily_input else 0,
                "intraday_snapshot_records": len(normalized_intraday_snapshot_input),
                "orderbook_records": len(normalized_orderbook_input),
                "trade_tick_records": len(normalized_trade_tick_input),
            },
            "normalized_daily_input": normalized_daily_input,
            "normalized_intraday_snapshot_input": normalized_intraday_snapshot_input,
            "normalized_orderbook_input": normalized_orderbook_input,
            "normalized_trade_tick_input": normalized_trade_tick_input,
        }

    def _normalize_daily_record(
        self,
        raw_record: dict[str, Any],
        fallback_symbol: str,
        fallback_trading_date: str,
    ) -> dict[str, Any]:
        trading_date = coerce_date_text(raw_record.get("date")) or fallback_trading_date
        return {
            "source_group": "historical_price",
            "symbol": normalize_symbol(raw_record.get("symbol"), fallback_symbol),
            "trading_date": trading_date,
            "price_open": to_float(raw_record.get("priceOpen")),
            "price_high": to_float(raw_record.get("priceHigh")),
            "price_low": to_float(raw_record.get("priceLow")),
            "price_close": to_float(raw_record.get("priceClose")),
            "price_basic": to_float(raw_record.get("priceBasic")),
            "price_average": to_float(raw_record.get("priceAverage")),
            "total_volume": to_float(raw_record.get("totalVolume")),
            "deal_volume": to_float(raw_record.get("dealVolume")),
            "putthrough_volume": to_float(raw_record.get("putthroughVolume")),
            "total_value": to_float(raw_record.get("totalValue")),
            "putthrough_value": to_float(raw_record.get("putthroughValue")),
            "buy_foreign_quantity": to_float(raw_record.get("buyForeignQuantity")),
            "buy_foreign_value": to_float(raw_record.get("buyForeignValue")),
            "sell_foreign_quantity": to_float(raw_record.get("sellForeignQuantity")),
            "sell_foreign_value": to_float(raw_record.get("sellForeignValue")),
            "buy_count": to_float(raw_record.get("buyCount")),
            "buy_quantity": to_float(raw_record.get("buyQuantity")),
            "sell_count": to_float(raw_record.get("sellCount")),
            "sell_quantity": to_float(raw_record.get("sellQuantity")),
            "adj_ratio": to_float(raw_record.get("adjRatio")),
            "current_foreign_room": to_float(raw_record.get("currentForeignRoom")),
            "prop_net_deal_value": to_float(raw_record.get("propTradingNetDealValue")),
            "prop_net_pt_value": to_float(raw_record.get("propTradingNetPTValue")),
            "prop_net_value": to_float(raw_record.get("propTradingNetValue")),
            "unit": to_float(raw_record.get("unit")),
        }

    def _normalize_intraday_snapshot_record(
        self,
        raw_record: dict[str, Any],
        fallback_symbol: str,
        fallback_trading_date: str,
        source_sequence: int,
    ) -> dict[str, Any]:
        local_event_time = parse_event_time_utc(raw_record.get("event_time"))
        reference_trading_date = (
            coerce_date_text(local_event_time.astimezone(LOCAL_TRADING_TZ).isoformat())
            if local_event_time is not None
            else fallback_trading_date
        )
        normalized = {
            "source_group": "updatelastprices",
            "source_sequence": source_sequence,
            "symbol": normalize_symbol(raw_record.get("symbol"), fallback_symbol),
            "reference_trading_date_local": reference_trading_date or fallback_trading_date,
            "command": raw_record.get("command"),
            "last_price": to_float(raw_record.get("last_price")),
            "last_volume": to_float(raw_record.get("last_volume")),
            "deal_volume": to_float(raw_record.get("deal_volume")),
            "deal_value": to_float(raw_record.get("deal_value")),
            "matched_price": to_float(raw_record.get("matched_price")),
            "active_buy_volume": to_float(raw_record.get("active_buy_volume")),
            "active_sell_volume": to_float(raw_record.get("active_sell_volume")),
            "total_buy_sale_volume": to_float(raw_record.get("total_buy_sale_volume")),
        }
        normalized.update(build_time_fields(raw_record))
        return normalized

    def _normalize_orderbook_record(
        self,
        raw_record: dict[str, Any],
        fallback_symbol: str,
        fallback_trading_date: str,
        source_sequence: int,
    ) -> dict[str, Any]:
        local_event_time = parse_event_time_utc(raw_record.get("event_time"))
        reference_trading_date = (
            coerce_date_text(local_event_time.astimezone(LOCAL_TRADING_TZ).isoformat())
            if local_event_time is not None
            else fallback_trading_date
        )
        normalized = {
            "source_group": "updateorderbooks",
            "source_sequence": source_sequence,
            "symbol": normalize_symbol(raw_record.get("symbol"), fallback_symbol),
            "reference_trading_date_local": reference_trading_date or fallback_trading_date,
            "command": raw_record.get("command"),
            "reported_total_bid_volume": to_float(raw_record.get("total_bid_volume")),
            "reported_total_ask_volume": to_float(raw_record.get("total_ask_volume")),
            "bid_levels": normalize_levels(raw_record.get("bid_levels")),
            "ask_levels": normalize_levels(raw_record.get("ask_levels")),
        }
        normalized.update(build_time_fields(raw_record))
        return normalized

    def _normalize_trade_record(
        self,
        raw_record: dict[str, Any],
        fallback_symbol: str,
        fallback_trading_date: str,
        source_sequence: int,
    ) -> dict[str, Any]:
        normalized = {
            "source_group": "updatetrades",
            "source_sequence": source_sequence,
            "symbol": normalize_symbol(raw_record.get("symbol"), fallback_symbol),
            "trade_id": to_int(raw_record.get("trade_id")),
            "reference_trading_date_local": coerce_date_text(raw_record.get("trading_date")) or fallback_trading_date,
            "command": raw_record.get("command"),
            "price": to_float(raw_record.get("price")),
            "volume": to_float(raw_record.get("volume")),
            "side": normalize_side(raw_record.get("side")),
        }
        normalized.update(build_time_fields(raw_record))
        return normalized

    def _log_event(self, symbol: str, trading_date: str, payload: dict[str, Any]) -> None:
        return

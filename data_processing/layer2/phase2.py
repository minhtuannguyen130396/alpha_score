from __future__ import annotations

import json
import math
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any

from layer2.phase0 import (
    FileSignature,
    SESSION_END_LOCAL,
    coerce_date_text,
    default_output_root,
    sanitize_symbol,
    utc_now_iso,
)
from layer2.phase1 import BUNDLE_FOLDER as PHASE1_BUNDLE_FOLDER, load_json_object


INPUT_FOLDER = PHASE1_BUNDLE_FOLDER
BUNDLE_FOLDER = "phase2_input_validation"
LOG_FOLDER = "phase2_input_validation_logs"


@dataclass
class CachedBundleFile:
    symbol: str
    trading_date: str
    path: Path
    signature: FileSignature
    payload: dict[str, Any]


def output_bundle_path(output_root: Path, symbol: str, trading_date: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / BUNDLE_FOLDER / trading_date[:4] / f"{trading_date}.json"


def output_log_path(output_root: Path, symbol: str, trading_date: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / LOG_FOLDER / trading_date[:4] / f"{trading_date}.jsonl"


def record_sort_key(record: dict[str, Any]) -> tuple[str, str, int]:
    return (
        str(record.get("event_time_utc") or ""),
        str(record.get("ingested_at_utc") or record.get("ingested_at_local") or ""),
        int(record.get("source_sequence") or 0),
    )


def approx_equal(left: Any, right: Any, abs_tol: float) -> bool:
    if left is None or right is None:
        return False
    return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=abs_tol)


def make_rule_result(
    rule_id: str,
    scope: str,
    status: str,
    checked_count: int,
    pass_count: int = 0,
    fail_count: int = 0,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "scope": scope,
        "status": status,
        "checked_count": checked_count,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "details": details or {},
    }


def parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None


class Phase2InputValidationProcess:
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
            f"starting phase2 input validation: output={self.output_root} interval={interval_seconds}s"
        )
        while True:
            self.run_once()
            time.sleep(interval_seconds)

    def run_once(self) -> dict[str, int]:
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
                self._stdout(f"failed validation bundle for {symbol} {trading_date}: {exc}")

        if not dirty_items:
            self._stdout("phase2 cycle complete: idle")
        else:
            self._stdout(
                f"phase2 cycle complete: dirty={len(dirty_items)} built={built_count} removed={removed_count} failed={failed_count}"
            )

        return {
            "dirty": len(dirty_items),
            "built": built_count,
            "removed": removed_count,
            "failed": failed_count,
        }

    def _stdout(self, message: str) -> None:
        print(f"[{utc_now_iso()}] {message}")

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
                symbol_value = sanitize_symbol(str(payload.get("symbol") or symbol).upper())
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
                self._stdout(f"removed stale phase2 bundle: {symbol} {trading_date}")
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
                "validation_status": bundle["input_quality"]["validation_status"],
                "rule_status_counts": bundle["input_quality"]["rule_status_counts"],
                "degraded_flags": bundle["degraded_flags"],
            },
        )
        self._stdout(
            f"built phase2 bundle: {symbol} {trading_date} status={bundle['input_quality']['validation_status']}"
        )
        return "built"

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        source_bundle = self._find_source_bundle(symbol, trading_date)
        if source_bundle is None:
            return None

        payload = source_bundle.payload
        normalized_daily_input = (
            payload.get("normalized_daily_input")
            if isinstance(payload.get("normalized_daily_input"), dict)
            else None
        )
        intraday_records = [
            record
            for record in (payload.get("normalized_intraday_snapshot_input") or [])
            if isinstance(record, dict)
        ]
        orderbook_records = [
            record
            for record in (payload.get("normalized_orderbook_input") or [])
            if isinstance(record, dict)
        ]
        trade_records = [
            record
            for record in (payload.get("normalized_trade_tick_input") or [])
            if isinstance(record, dict)
        ]

        intraday_records.sort(key=record_sort_key)
        orderbook_records.sort(key=record_sort_key)
        trade_records.sort(key=record_sort_key)

        rule_results = [
            self._validate_daily_price_range(normalized_daily_input),
            self._validate_daily_volume_balance(normalized_daily_input),
            self._validate_daily_average_price(normalized_daily_input),
            self._validate_daily_prop_net_balance(normalized_daily_input),
            self._validate_intraday_last_price_match(intraday_records),
            self._validate_intraday_deal_volume_balance(intraday_records),
            self._validate_orderbook_top3_shape(orderbook_records),
            self._validate_orderbook_total_depth_usability(orderbook_records),
            self._validate_trade_tick_integrity(trade_records),
            self._validate_cross_source_eod_deal_volume(normalized_daily_input, intraday_records),
            self._validate_cross_source_eod_close_price(normalized_daily_input, intraday_records),
            self._validate_cross_source_eod_deal_value(normalized_daily_input, intraday_records),
        ]

        null_trade_side_count = sum(1 for record in trade_records if record.get("side") is None)
        unusable_total_depth = False
        if orderbook_records:
            unusable_total_depth = all(
                (record.get("reported_total_bid_volume") in (None, 0.0))
                and (record.get("reported_total_ask_volume") in (None, 0.0))
                for record in orderbook_records
            )

        degraded_flags = {
            "book_total_depth_unusable": unusable_total_depth,
            "trade_side_nullable": null_trade_side_count > 0,
        }

        rule_status_counts = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}
        for result in rule_results:
            status = result["status"]
            rule_status_counts[status] = rule_status_counts.get(status, 0) + 1

        if rule_status_counts["fail"] > 0:
            validation_status = "fail"
        elif rule_status_counts["warn"] > 0 or any(degraded_flags.values()):
            validation_status = "degraded"
        else:
            validation_status = "pass"

        latest_intraday_record = intraday_records[-1] if intraday_records else None

        return {
            "bundle_type": "validated_input_bundle",
            "phase": "phase2_input_validation",
            "symbol": symbol,
            "trading_date": trading_date,
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "source_group_status": payload.get("source_group_status"),
            "session_filter_local": payload.get("session_filter_local"),
            "normalization_metadata": payload.get("normalization_metadata"),
            "normalized_record_counts": payload.get("normalized_record_counts"),
            "normalized_daily_input": normalized_daily_input,
            "normalized_intraday_snapshot_input": intraday_records,
            "normalized_orderbook_input": orderbook_records,
            "normalized_trade_tick_input": trade_records,
            "input_validation_report": {
                "validated_at_utc": utc_now_iso(),
                "summary": {
                    "rule_status_counts": rule_status_counts,
                    "total_rules": len(rule_results),
                    "latest_intraday_event_time_utc": (
                        latest_intraday_record.get("event_time_utc") if latest_intraday_record else None
                    ),
                },
                "daily_rules": self._group_status_map(rule_results, "daily"),
                "intraday_rules": self._group_status_map(rule_results, "intraday"),
                "orderbook_rules": self._group_status_map(rule_results, "orderbook"),
                "trade_rules": self._group_status_map(rule_results, "trade"),
                "cross_source_rules": self._group_status_map(rule_results, "cross_source"),
                "rule_results": rule_results,
            },
            "degraded_flags": degraded_flags,
            "input_quality": {
                "validation_status": validation_status,
                "rule_status_counts": rule_status_counts,
                "degraded_flag_count": sum(1 for value in degraded_flags.values() if value),
                "null_trade_side_count": null_trade_side_count,
                "hard_fail_rule_ids": [
                    result["rule_id"] for result in rule_results if result["status"] == "fail"
                ],
                "warn_rule_ids": [
                    result["rule_id"] for result in rule_results if result["status"] == "warn"
                ],
            },
        }

    def _group_status_map(
        self,
        rule_results: list[dict[str, Any]],
        scope_prefix: str,
    ) -> dict[str, str]:
        return {
            result["rule_id"]: result["status"]
            for result in rule_results
            if result["scope"].startswith(scope_prefix)
        }

    def _latest_intraday_record(self, intraday_records: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not intraday_records:
            return None
        return intraday_records[-1]

    def _has_near_close_intraday_coverage(
        self,
        latest_record: dict[str, Any] | None,
        max_gap_seconds: int = 120,
    ) -> tuple[bool, dict[str, Any]]:
        if latest_record is None:
            return False, {"reason": "missing_intraday_snapshot"}

        event_time_local = parse_iso_datetime(latest_record.get("event_time_local"))
        if event_time_local is None:
            return False, {
                "reason": "missing_event_time_local",
                "latest_intraday_event_time_utc": latest_record.get("event_time_utc"),
            }

        event_local_time = event_time_local.timetz().replace(tzinfo=None)
        session_end_seconds = (
            SESSION_END_LOCAL.hour * 3600 + SESSION_END_LOCAL.minute * 60 + SESSION_END_LOCAL.second
        )
        latest_seconds = (
            event_local_time.hour * 3600 + event_local_time.minute * 60 + event_local_time.second
        )
        gap_seconds = session_end_seconds - latest_seconds
        has_coverage = 0 <= gap_seconds <= max_gap_seconds
        return has_coverage, {
            "latest_intraday_event_time_utc": latest_record.get("event_time_utc"),
            "latest_intraday_event_time_local": latest_record.get("event_time_local"),
            "session_end_local": dt_time(
                hour=SESSION_END_LOCAL.hour,
                minute=SESSION_END_LOCAL.minute,
                second=SESSION_END_LOCAL.second,
            ).isoformat(),
            "close_gap_seconds": gap_seconds,
            "required_max_gap_seconds": max_gap_seconds,
        }

    def _validate_daily_price_range(self, daily: dict[str, Any] | None) -> dict[str, Any]:
        if not daily or daily.get("price_high") is None or daily.get("price_low") is None:
            return make_rule_result("price_high_gte_price_low", "daily", "skip", 0)

        price_high = float(daily["price_high"])
        price_low = float(daily["price_low"])
        status = "pass" if price_high >= price_low else "fail"
        return make_rule_result(
            "price_high_gte_price_low",
            "daily",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={"price_high": price_high, "price_low": price_low},
        )

    def _validate_daily_volume_balance(self, daily: dict[str, Any] | None) -> dict[str, Any]:
        if not daily:
            return make_rule_result("total_volume_matches_components", "daily", "skip", 0)

        total_volume = daily.get("total_volume")
        deal_volume = daily.get("deal_volume")
        putthrough_volume = daily.get("putthrough_volume")
        if total_volume is None or deal_volume is None or putthrough_volume is None:
            return make_rule_result("total_volume_matches_components", "daily", "skip", 0)

        expected = float(deal_volume) + float(putthrough_volume)
        actual = float(total_volume)
        diff = actual - expected
        status = "pass" if approx_equal(actual, expected, abs_tol=1.0) else "fail"
        return make_rule_result(
            "total_volume_matches_components",
            "daily",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={"actual": actual, "expected": expected, "abs_diff": abs(diff)},
        )

    def _validate_daily_average_price(self, daily: dict[str, Any] | None) -> dict[str, Any]:
        if not daily:
            return make_rule_result("average_price_consistency", "daily", "skip", 0)

        price_average = daily.get("price_average")
        total_value = daily.get("total_value")
        putthrough_value = daily.get("putthrough_value")
        deal_volume = daily.get("deal_volume")
        unit = daily.get("unit")
        if (
            price_average is None
            or total_value is None
            or putthrough_value is None
            or deal_volume in (None, 0.0)
            or unit in (None, 0.0)
        ):
            return make_rule_result("average_price_consistency", "daily", "skip", 0)

        expected = (float(total_value) - float(putthrough_value)) / float(deal_volume) / float(unit)
        actual = float(price_average)
        diff = actual - expected
        status = "pass" if approx_equal(actual, expected, abs_tol=0.01) else "fail"
        return make_rule_result(
            "average_price_consistency",
            "daily",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={"actual": actual, "expected": expected, "abs_diff": abs(diff)},
        )

    def _validate_daily_prop_net_balance(self, daily: dict[str, Any] | None) -> dict[str, Any]:
        if not daily:
            return make_rule_result("prop_net_value_consistency", "daily", "skip", 0)

        prop_net_value = daily.get("prop_net_value")
        prop_net_deal_value = daily.get("prop_net_deal_value")
        prop_net_pt_value = daily.get("prop_net_pt_value")
        if prop_net_value is None or prop_net_deal_value is None or prop_net_pt_value is None:
            return make_rule_result("prop_net_value_consistency", "daily", "skip", 0)

        expected = float(prop_net_deal_value) + float(prop_net_pt_value)
        actual = float(prop_net_value)
        diff = actual - expected
        status = "pass" if approx_equal(actual, expected, abs_tol=1.0) else "fail"
        return make_rule_result(
            "prop_net_value_consistency",
            "daily",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={"actual": actual, "expected": expected, "abs_diff": abs(diff)},
        )

    def _validate_intraday_last_price_match(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        checked_count = 0
        fail_count = 0
        examples: list[dict[str, Any]] = []
        max_abs_diff = 0.0

        for record in records:
            last_price = record.get("last_price")
            matched_price = record.get("matched_price")
            if last_price is None or matched_price is None:
                continue

            checked_count += 1
            abs_diff = abs(float(last_price) - float(matched_price))
            max_abs_diff = max(max_abs_diff, abs_diff)
            if not approx_equal(last_price, matched_price, abs_tol=0.001):
                fail_count += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "source_sequence": record.get("source_sequence"),
                            "event_time_utc": record.get("event_time_utc"),
                            "last_price": last_price,
                            "matched_price": matched_price,
                            "abs_diff": abs_diff,
                        }
                    )

        if checked_count == 0:
            return make_rule_result("last_price_equals_matched_price", "intraday", "skip", 0)

        status = "pass" if fail_count == 0 else "fail"
        return make_rule_result(
            "last_price_equals_matched_price",
            "intraday",
            status,
            checked_count=checked_count,
            pass_count=checked_count - fail_count,
            fail_count=fail_count,
            details={"max_abs_diff": max_abs_diff, "examples": examples},
        )

    def _validate_intraday_deal_volume_balance(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        checked_count = 0
        fail_count = 0
        examples: list[dict[str, Any]] = []
        max_abs_diff = 0.0

        for record in records:
            deal_volume = record.get("deal_volume")
            active_buy_volume = record.get("active_buy_volume")
            active_sell_volume = record.get("active_sell_volume")
            if deal_volume is None or active_buy_volume is None or active_sell_volume is None:
                continue

            checked_count += 1
            expected = float(active_buy_volume) + float(active_sell_volume)
            actual = float(deal_volume)
            abs_diff = abs(actual - expected)
            max_abs_diff = max(max_abs_diff, abs_diff)
            if not approx_equal(actual, expected, abs_tol=1.0):
                fail_count += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "source_sequence": record.get("source_sequence"),
                            "event_time_utc": record.get("event_time_utc"),
                            "deal_volume": deal_volume,
                            "expected": expected,
                            "abs_diff": abs_diff,
                        }
                    )

        if checked_count == 0:
            return make_rule_result(
                "deal_volume_equals_active_buy_plus_sell",
                "intraday",
                "skip",
                0,
            )

        status = "pass" if fail_count == 0 else "fail"
        return make_rule_result(
            "deal_volume_equals_active_buy_plus_sell",
            "intraday",
            status,
            checked_count=checked_count,
            pass_count=checked_count - fail_count,
            fail_count=fail_count,
            details={"max_abs_diff": max_abs_diff, "examples": examples},
        )

    def _validate_orderbook_top3_shape(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        checked_count = len(records)
        if checked_count == 0:
            return make_rule_result("book_has_exactly_3_levels_each_side", "orderbook", "skip", 0)

        fail_count = 0
        examples: list[dict[str, Any]] = []
        for record in records:
            bid_count = len(record.get("bid_levels") or [])
            ask_count = len(record.get("ask_levels") or [])
            if bid_count != 3 or ask_count != 3:
                fail_count += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "source_sequence": record.get("source_sequence"),
                            "event_time_utc": record.get("event_time_utc"),
                            "bid_level_count": bid_count,
                            "ask_level_count": ask_count,
                        }
                    )

        status = "pass" if fail_count == 0 else "fail"
        return make_rule_result(
            "book_has_exactly_3_levels_each_side",
            "orderbook",
            status,
            checked_count=checked_count,
            pass_count=checked_count - fail_count,
            fail_count=fail_count,
            details={"examples": examples},
        )

    def _validate_orderbook_total_depth_usability(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        checked_count = len(records)
        if checked_count == 0:
            return make_rule_result("reported_total_depth_usable", "orderbook", "skip", 0)

        unusable_count = sum(
            1
            for record in records
            if (record.get("reported_total_bid_volume") in (None, 0.0))
            and (record.get("reported_total_ask_volume") in (None, 0.0))
        )
        status = "warn" if unusable_count == checked_count else "pass"
        return make_rule_result(
            "reported_total_depth_usable",
            "orderbook",
            status,
            checked_count=checked_count,
            pass_count=checked_count if status == "pass" else 0,
            fail_count=0,
            details={
                "unusable_snapshot_count": unusable_count,
                "reported_unusable_ratio": (unusable_count / checked_count) if checked_count else None,
            },
        )

    def _validate_trade_tick_integrity(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        checked_count = len(records)
        if checked_count == 0:
            return make_rule_result("trade_tick_basic_integrity", "trade", "skip", 0)

        invalid_trade_id = 0
        non_positive_price = 0
        non_positive_volume = 0
        invalid_side = 0
        null_side = 0
        invalid_record_count = 0
        examples: list[dict[str, Any]] = []

        for record in records:
            trade_id = record.get("trade_id")
            price = record.get("price")
            volume = record.get("volume")
            side = record.get("side")

            is_invalid = False
            if trade_id is None:
                invalid_trade_id += 1
                is_invalid = True
            if price is None or float(price) <= 0:
                non_positive_price += 1
                is_invalid = True
            if volume is None or float(volume) <= 0:
                non_positive_volume += 1
                is_invalid = True
            if side is None:
                null_side += 1
            elif side not in {"B", "S"}:
                invalid_side += 1
                is_invalid = True

            if is_invalid:
                invalid_record_count += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "source_sequence": record.get("source_sequence"),
                            "event_time_utc": record.get("event_time_utc"),
                            "trade_id": trade_id,
                            "price": price,
                            "volume": volume,
                            "side": side,
                        }
                    )

        status = "pass" if invalid_record_count == 0 else "fail"
        return make_rule_result(
            "trade_tick_basic_integrity",
            "trade",
            status,
            checked_count=checked_count,
            pass_count=checked_count - invalid_record_count,
            fail_count=invalid_record_count,
            details={
                "invalid_trade_id_count": invalid_trade_id,
                "non_positive_price_count": non_positive_price,
                "non_positive_volume_count": non_positive_volume,
                "invalid_side_count": invalid_side,
                "null_side_count": null_side,
                "examples": examples,
            },
        )

    def _validate_cross_source_eod_deal_volume(
        self,
        daily: dict[str, Any] | None,
        intraday_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        latest_record = self._latest_intraday_record(intraday_records)
        if not daily or not latest_record:
            return make_rule_result("eod_deal_volume_matches_daily", "cross_source", "skip", 0)

        has_coverage, coverage_details = self._has_near_close_intraday_coverage(latest_record)
        if not has_coverage:
            return make_rule_result(
                "eod_deal_volume_matches_daily",
                "cross_source",
                "warn",
                checked_count=0,
                details=coverage_details,
            )

        daily_deal_volume = daily.get("deal_volume")
        latest_deal_volume = latest_record.get("deal_volume")
        if daily_deal_volume is None or latest_deal_volume is None:
            return make_rule_result("eod_deal_volume_matches_daily", "cross_source", "skip", 0)

        actual = float(latest_deal_volume)
        expected = float(daily_deal_volume)
        diff = actual - expected
        status = "pass" if approx_equal(actual, expected, abs_tol=1.0) else "fail"
        return make_rule_result(
            "eod_deal_volume_matches_daily",
            "cross_source",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={
                **coverage_details,
                "actual": actual,
                "expected": expected,
                "abs_diff": abs(diff),
            },
        )

    def _validate_cross_source_eod_close_price(
        self,
        daily: dict[str, Any] | None,
        intraday_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        latest_record = self._latest_intraday_record(intraday_records)
        if not daily or not latest_record:
            return make_rule_result("eod_close_matches_daily_close", "cross_source", "skip", 0)

        has_coverage, coverage_details = self._has_near_close_intraday_coverage(latest_record)
        if not has_coverage:
            return make_rule_result(
                "eod_close_matches_daily_close",
                "cross_source",
                "warn",
                checked_count=0,
                details=coverage_details,
            )

        price_close = daily.get("price_close")
        latest_last_price = latest_record.get("last_price")
        if price_close is None or latest_last_price is None:
            return make_rule_result("eod_close_matches_daily_close", "cross_source", "skip", 0)

        actual = float(latest_last_price)
        expected = float(price_close)
        diff = actual - expected
        status = "pass" if approx_equal(actual, expected, abs_tol=0.001) else "fail"
        return make_rule_result(
            "eod_close_matches_daily_close",
            "cross_source",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={
                **coverage_details,
                "actual": actual,
                "expected": expected,
                "abs_diff": abs(diff),
            },
        )

    def _validate_cross_source_eod_deal_value(
        self,
        daily: dict[str, Any] | None,
        intraday_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        latest_record = self._latest_intraday_record(intraday_records)
        if not daily or not latest_record:
            return make_rule_result("eod_deal_value_matches_daily_deal_value", "cross_source", "skip", 0)

        has_coverage, coverage_details = self._has_near_close_intraday_coverage(latest_record)
        if not has_coverage:
            return make_rule_result(
                "eod_deal_value_matches_daily_deal_value",
                "cross_source",
                "warn",
                checked_count=0,
                details=coverage_details,
            )

        total_value = daily.get("total_value")
        putthrough_value = daily.get("putthrough_value")
        latest_deal_value = latest_record.get("deal_value")
        if total_value is None or putthrough_value is None or latest_deal_value is None:
            return make_rule_result("eod_deal_value_matches_daily_deal_value", "cross_source", "skip", 0)

        expected = float(total_value) - float(putthrough_value)
        actual = float(latest_deal_value)
        diff = actual - expected
        status = "pass" if approx_equal(actual, expected, abs_tol=50000.0) else "fail"
        return make_rule_result(
            "eod_deal_value_matches_daily_deal_value",
            "cross_source",
            status,
            checked_count=1,
            pass_count=1 if status == "pass" else 0,
            fail_count=1 if status == "fail" else 0,
            details={
                **coverage_details,
                "actual": actual,
                "expected": expected,
                "abs_diff": abs(diff),
            },
        )

    def _log_event(self, symbol: str, trading_date: str, payload: dict[str, Any]) -> None:
        path = output_log_path(self.output_root, symbol, trading_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

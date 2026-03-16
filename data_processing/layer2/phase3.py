from __future__ import annotations

import json
import time
import traceback
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from layer2.phase0 import FileSignature, coerce_date_text, default_output_root, sanitize_symbol, utc_now_iso
from layer2.phase1 import load_json_object
from layer2.phase2 import BUNDLE_FOLDER as PHASE2_BUNDLE_FOLDER, parse_iso_datetime


INPUT_FOLDER = PHASE2_BUNDLE_FOLDER
BUNDLE_FOLDER = "phase3_time_alignment_reference_indexing"
LOG_FOLDER = "phase3_time_alignment_reference_indexing_logs"
TRADE_WINDOWS_SECONDS = {
    "1s": 1,
    "5s": 5,
    "10s": 10,
    "60s": 60,
    "5m": 300,
}


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


def age_ms(reference_time: datetime, observed_time: datetime | None) -> int | None:
    if observed_time is None:
        return None
    return max(int((reference_time - observed_time).total_seconds() * 1000), 0)


def build_index_range(start_index: int, end_index: int) -> dict[str, Any]:
    if end_index < start_index:
        return {
            "start_index": None,
            "end_index": None,
            "count": 0,
        }
    return {
        "start_index": start_index,
        "end_index": end_index,
        "count": end_index - start_index + 1,
    }


class Phase3TimeAlignmentProcess:
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
            f"starting phase3 time alignment: output={self.output_root} interval={interval_seconds}s"
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
                self._stdout(f"failed phase3 bundle for {symbol} {trading_date}: {exc}")

        if not dirty_items:
            self._stdout("phase3 cycle complete: idle")
        else:
            self._stdout(
                f"phase3 cycle complete: dirty={len(dirty_items)} built={built_count} removed={removed_count} failed={failed_count}"
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
                self._stdout(f"removed stale phase3 bundle: {symbol} {trading_date}")
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
                "reference_frame_count": bundle["reference_frame_count"],
                "last_reference_time_local": bundle["reference_time_summary"]["last_reference_time_local"],
            },
        )
        self._stdout(
            f"built phase3 bundle: {symbol} {trading_date} frames={bundle['reference_frame_count']}"
        )
        return "built"

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        source_bundle = self._find_source_bundle(symbol, trading_date)
        if source_bundle is None:
            return None

        payload = source_bundle.payload
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

        if not intraday_records:
            return None

        intraday_times_local = [parse_iso_datetime(record.get("event_time_local")) for record in intraday_records]
        intraday_times_utc = [parse_iso_datetime(record.get("event_time_utc")) for record in intraday_records]
        orderbook_times_local = [parse_iso_datetime(record.get("event_time_local")) for record in orderbook_records]
        trade_times_local = [parse_iso_datetime(record.get("event_time_local")) for record in trade_records]

        valid_orderbook_times = [time_value for time_value in orderbook_times_local if time_value is not None]
        valid_trade_times = [time_value for time_value in trade_times_local if time_value is not None]
        if len(valid_orderbook_times) != len(orderbook_times_local) or len(valid_trade_times) != len(trade_times_local):
            raise ValueError("phase3 requires all orderbook and trade records to have parseable event_time_local")

        reference_frames: list[dict[str, Any]] = []
        last_trade_index = None
        for intraday_index, reference_time_local in enumerate(intraday_times_local):
            reference_time_utc = intraday_times_utc[intraday_index]
            if reference_time_local is None or reference_time_utc is None:
                continue

            orderbook_index = bisect_right(valid_orderbook_times, reference_time_local) - 1
            if orderbook_index < 0:
                orderbook_index = None

            trade_index = bisect_right(valid_trade_times, reference_time_local) - 1
            if trade_index < 0:
                trade_index = None

            trade_window_ranges: dict[str, dict[str, Any]] = {}
            for label, seconds in TRADE_WINDOWS_SECONDS.items():
                if trade_index is None:
                    trade_window_ranges[label] = build_index_range(0, -1)
                    continue

                window_start_time = reference_time_local - timedelta(seconds=seconds)
                start_index = bisect_right(valid_trade_times, window_start_time)
                trade_window_ranges[label] = build_index_range(start_index, trade_index)

            orderbook_time = valid_orderbook_times[orderbook_index] if orderbook_index is not None else None
            last_trade_time = valid_trade_times[trade_index] if trade_index is not None else None

            reference_frames.append(
                {
                    "frame_no": len(reference_frames) + 1,
                    "reference_time_local": reference_time_local.isoformat(),
                    "reference_time_utc": reference_time_utc.isoformat(),
                    "daily_context_available": isinstance(payload.get("normalized_daily_input"), dict),
                    "intraday_snapshot_index": intraday_index,
                    "orderbook_snapshot_index": orderbook_index,
                    "last_trade_index": trade_index,
                    "trade_window_index_ranges": trade_window_ranges,
                    "age_metadata": {
                        "book_age_ms": age_ms(reference_time_local, orderbook_time),
                        "tick_age_ms": age_ms(reference_time_local, last_trade_time),
                        "intraday_snapshot_age_ms": age_ms(reference_time_local, reference_time_local),
                    },
                    "delta_trade_index_range": build_index_range(
                        0 if last_trade_index is None else last_trade_index + 1,
                        -1 if trade_index is None else trade_index,
                    ),
                }
            )
            last_trade_index = trade_index

        if not reference_frames:
            return None

        return {
            "bundle_type": "aligned_input_frame",
            "phase": "phase3_time_alignment_reference_indexing",
            "symbol": symbol,
            "trading_date": trading_date,
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "validation_status": payload.get("input_quality", {}).get("validation_status"),
            "degraded_flags": payload.get("degraded_flags"),
            "session_filter_local": payload.get("session_filter_local"),
            "window_definitions_seconds": TRADE_WINDOWS_SECONDS,
            "reference_axis_source": "normalized_intraday_snapshot_input.event_time_local",
            "reference_frame_count": len(reference_frames),
            "reference_time_summary": {
                "first_reference_time_local": reference_frames[0]["reference_time_local"],
                "last_reference_time_local": reference_frames[-1]["reference_time_local"],
            },
            "aligned_sources": {
                "daily_context": payload.get("normalized_daily_input"),
                "intraday_snapshots": intraday_records,
                "orderbook_snapshots": orderbook_records,
                "trade_ticks": trade_records,
            },
            "reference_frames": reference_frames,
        }

    def _log_event(self, symbol: str, trading_date: str, payload: dict[str, Any]) -> None:
        path = output_log_path(self.output_root, symbol, trading_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

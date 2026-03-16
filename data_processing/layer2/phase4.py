from __future__ import annotations

import json
import shutil
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from layer2.phase0 import FileSignature, coerce_date_text, default_output_root, sanitize_symbol, utc_now_iso
from layer2.phase1 import load_json_object
from layer2.phase2 import parse_iso_datetime
from layer2.phase3 import BUNDLE_FOLDER as PHASE3_BUNDLE_FOLDER


INPUT_FOLDER = PHASE3_BUNDLE_FOLDER
BUNDLE_FOLDER = "phase4_book_memory_deep_reconstruction"
LOG_FOLDER = "phase4_book_memory_deep_reconstruction_logs"
MEMORY_EXPIRY_MS = 300_000
CARRY_WINDOW_MS = 15_000
FAKE_WALL_THRESHOLD = 0.75
FAKE_WALL_MIN_STALE_MS = 60_000
WALL_SIZE_THRESHOLD = 100_000.0


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


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def price_token(value: Any) -> float | None:
    numeric = to_float(value)
    if numeric is None:
        return None
    return round(numeric, 6)


def visible_levels(snapshot: dict[str, Any] | None) -> dict[tuple[str, float], float]:
    if not isinstance(snapshot, dict):
        return {}

    current: dict[tuple[str, float], float] = {}
    for side_name, field_name in (("bid", "bid_levels"), ("ask", "ask_levels")):
        for level in snapshot.get(field_name) or []:
            if not isinstance(level, dict):
                continue
            price = price_token(level.get("price"))
            volume = to_float(level.get("volume"))
            if price is None or volume is None:
                continue
            current[(side_name, price)] = volume
    return current


def expected_trade_side(book_side: str) -> str:
    return "S" if book_side == "bid" else "B"


def matching_executed_volume(
    trades: list[dict[str, Any]],
    book_side: str,
    target_price: float,
) -> float:
    matched = 0.0
    required_trade_side = expected_trade_side(book_side)
    for trade in trades:
        trade_price = price_token(trade.get("price"))
        trade_volume = to_float(trade.get("volume"))
        trade_side = trade.get("side")
        if trade_price is None or trade_volume is None:
            continue
        if trade_price != target_price:
            continue
        if trade_side not in (required_trade_side, None):
            continue
        matched += trade_volume
    return matched


def export_memory_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": entry["symbol"],
        "side": entry["side"],
        "price": entry["price"],
        "last_observed_volume": entry["last_observed_volume"],
        "max_observed_volume": entry["max_observed_volume"],
        "first_seen_event_time": entry["first_seen_event_time"],
        "last_seen_event_time": entry["last_seen_event_time"],
        "carry_forward_volume": entry["carry_forward_volume"],
        "stale_age_ms": entry["stale_age_ms"],
        "reappeared_count": entry["reappeared_count"],
        "executed_volume_at_price": entry["executed_volume_at_price"],
        "disappeared_volume": entry["disappeared_volume"],
        "trade_explained_ratio": entry["trade_explained_ratio"],
        "cancel_inferred_volume": entry["cancel_inferred_volume"],
        "wall_suspicion_score": entry["wall_suspicion_score"],
        "reconstruction_confidence": entry["reconstruction_confidence"],
        "state": entry["state"],
    }


class Phase4DeepBookReconstructionProcess:
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
            f"starting phase4 deep book reconstruction: output={self.output_root} interval={interval_seconds}s"
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
                self._stdout(f"failed phase4 bundle for {symbol} {trading_date}: {exc}")

        if not dirty_items:
            self._stdout("phase4 cycle complete: idle")
        else:
            self._stdout(
                f"phase4 cycle complete: dirty={len(dirty_items)} built={built_count} removed={removed_count} failed={failed_count}"
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
                self._stdout(f"removed stale phase4 bundle: {symbol} {trading_date}")
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
                "frame_count": bundle["frame_count"],
                "frames_with_wall_candidates": bundle["reconstruction_quality"]["frames_with_wall_candidates"],
            },
        )
        self._stdout(
            f"built phase4 bundle: {symbol} {trading_date} frames={bundle['frame_count']}"
        )
        return "built"

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        source_bundle = self._find_source_bundle(symbol, trading_date)
        if source_bundle is None:
            return None

        payload = source_bundle.payload
        aligned_sources = payload.get("aligned_sources") or {}
        orderbook_snapshots = [
            snapshot for snapshot in (aligned_sources.get("orderbook_snapshots") or []) if isinstance(snapshot, dict)
        ]
        trade_ticks = [
            trade for trade in (aligned_sources.get("trade_ticks") or []) if isinstance(trade, dict)
        ]
        reference_frames = [
            frame for frame in (payload.get("reference_frames") or []) if isinstance(frame, dict)
        ]
        if not reference_frames:
            return None

        degraded_flags = payload.get("degraded_flags") or {}
        trade_side_nullable = bool(degraded_flags.get("trade_side_nullable"))
        book_total_depth_unusable = bool(degraded_flags.get("book_total_depth_unusable"))

        memory_state: dict[tuple[str, float], dict[str, Any]] = {}
        deep_book_frames: list[dict[str, Any]] = []
        frames_with_wall_candidates = 0

        for frame in reference_frames:
            reference_time_local = parse_iso_datetime(frame.get("reference_time_local"))
            reference_time_utc = frame.get("reference_time_utc")
            if reference_time_local is None:
                continue

            delta_range = frame.get("delta_trade_index_range") or {}
            delta_start = delta_range.get("start_index")
            delta_end = delta_range.get("end_index")
            delta_trades = []
            if delta_start is not None and delta_end is not None and delta_end >= delta_start:
                delta_trades = trade_ticks[delta_start : delta_end + 1]

            orderbook_index = frame.get("orderbook_snapshot_index")
            orderbook_snapshot = (
                orderbook_snapshots[orderbook_index]
                if isinstance(orderbook_index, int) and 0 <= orderbook_index < len(orderbook_snapshots)
                else None
            )
            current_levels = visible_levels(orderbook_snapshot)

            for entry in memory_state.values():
                entry["_observed_this_frame"] = False

            for (side, price), volume in current_levels.items():
                memory_key = (side, price)
                existing = memory_state.get(memory_key)
                if existing is None:
                    memory_state[memory_key] = self._new_observed_entry(
                        symbol=symbol,
                        side=side,
                        price=price,
                        volume=volume,
                        reference_time_local=reference_time_local,
                    )
                    continue

                state = "REAPPEARED" if existing["state"] != "OBSERVED_TOP3" else "OBSERVED_TOP3"
                if state == "REAPPEARED":
                    existing["reappeared_count"] += 1
                existing.update(
                    {
                        "last_observed_volume": volume,
                        "max_observed_volume": max(existing["max_observed_volume"], volume),
                        "last_seen_event_time": reference_time_local.isoformat(),
                        "carry_forward_volume": 0.0,
                        "stale_age_ms": 0,
                        "executed_volume_at_price": 0.0,
                        "disappeared_volume": 0.0,
                        "trade_explained_ratio": 1.0,
                        "cancel_inferred_volume": 0.0,
                        "wall_suspicion_score": 0.0,
                        "reconstruction_confidence": self._reconstruction_confidence(
                            state=state,
                            stale_age_ms=0,
                            book_total_depth_unusable=book_total_depth_unusable,
                            trade_side_nullable=trade_side_nullable,
                        ),
                        "state": state,
                        "_baseline_disappeared_volume": 0.0,
                        "_observed_this_frame": True,
                    }
                )

            expired_keys: list[tuple[str, float]] = []
            for memory_key, entry in list(memory_state.items()):
                if entry.get("_observed_this_frame"):
                    continue

                last_seen_time = parse_iso_datetime(entry.get("last_seen_event_time"))
                stale_age = 0 if last_seen_time is None else max(
                    int((reference_time_local - last_seen_time).total_seconds() * 1000),
                    0,
                )
                baseline = entry.get("_baseline_disappeared_volume") or entry.get("last_observed_volume") or 0.0
                if baseline <= 0:
                    baseline = 0.0
                if entry.get("_baseline_disappeared_volume", 0.0) <= 0.0:
                    entry["_baseline_disappeared_volume"] = baseline

                executed_delta = matching_executed_volume(delta_trades, entry["side"], entry["price"])
                executed_total = min(
                    baseline,
                    float(entry.get("executed_volume_at_price") or 0.0) + executed_delta,
                )
                trade_explained_ratio = 1.0 if baseline <= 0 else clamp(executed_total / baseline)
                cancel_inferred_volume = max(baseline - executed_total, 0.0)
                carry_forward_volume = max(baseline - executed_total, 0.0)
                wall_suspicion_score = self._wall_suspicion_score(
                    unexplained_ratio=1.0 - trade_explained_ratio,
                    max_observed_volume=float(entry.get("max_observed_volume") or 0.0),
                    reappeared_count=int(entry.get("reappeared_count") or 0),
                    stale_age_ms=stale_age,
                )
                state = self._missing_state(
                    stale_age_ms=stale_age,
                    trade_explained_ratio=trade_explained_ratio,
                    carry_forward_volume=carry_forward_volume,
                    max_observed_volume=float(entry.get("max_observed_volume") or 0.0),
                    wall_suspicion_score=wall_suspicion_score,
                    reappeared_count=int(entry.get("reappeared_count") or 0),
                )
                reconstruction_confidence = self._reconstruction_confidence(
                    state=state,
                    stale_age_ms=stale_age,
                    book_total_depth_unusable=book_total_depth_unusable,
                    trade_side_nullable=trade_side_nullable,
                )

                entry.update(
                    {
                        "carry_forward_volume": carry_forward_volume,
                        "stale_age_ms": stale_age,
                        "executed_volume_at_price": executed_total,
                        "disappeared_volume": baseline,
                        "trade_explained_ratio": trade_explained_ratio,
                        "cancel_inferred_volume": cancel_inferred_volume,
                        "wall_suspicion_score": wall_suspicion_score,
                        "reconstruction_confidence": reconstruction_confidence,
                        "state": state,
                        "_observed_this_frame": False,
                    }
                )
                if state == "EXPIRED":
                    expired_keys.append(memory_key)

            snapshot_entries = [
                export_memory_entry(entry)
                for entry in memory_state.values()
                if entry["state"] != "EXPIRED"
            ]
            expired_entries = [
                export_memory_entry(memory_state[memory_key])
                for memory_key in expired_keys
                if memory_key in memory_state
            ]
            snapshot_entries.extend(expired_entries)
            snapshot_entries.sort(key=lambda item: (item["side"], item["price"]))

            wall_candidates = [
                {
                    "price": entry["price"],
                    "side": entry["side"],
                    "state": entry["state"],
                    "wall_suspicion_score": entry["wall_suspicion_score"],
                    "reconstruction_confidence": entry["reconstruction_confidence"],
                }
                for entry in snapshot_entries
                if entry["state"] == "SUSPECTED_FAKE_WALL"
            ]
            if wall_candidates:
                frames_with_wall_candidates += 1

            deep_book_frames.append(
                {
                    "frame_no": frame.get("frame_no"),
                    "reference_time_local": frame.get("reference_time_local"),
                    "reference_time_utc": reference_time_utc,
                    "orderbook_snapshot_index": orderbook_index,
                    "memory_level_count": len(snapshot_entries),
                    "carried_level_count": sum(
                        1
                        for entry in snapshot_entries
                        if entry["state"] in {"CARRIED_OUTSIDE_TOP3", "PULLED_OR_CANCELLED", "SUSPECTED_FAKE_WALL"}
                    ),
                    "wall_candidate_count": len(wall_candidates),
                    "memory_levels": snapshot_entries,
                    "wall_candidates": wall_candidates,
                }
            )

            for memory_key in expired_keys:
                memory_state.pop(memory_key, None)

        latest_memory_levels = deep_book_frames[-1]["memory_levels"] if deep_book_frames else []
        return {
            "bundle_type": "deep_book_input_bundle",
            "phase": "phase4_book_memory_deep_reconstruction",
            "symbol": symbol,
            "trading_date": trading_date,
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "frame_count": len(deep_book_frames),
            "deep_book_frames": deep_book_frames,
            "latest_memory_levels": latest_memory_levels,
            "reconstruction_quality": {
                "is_reconstructed_input": True,
                "is_full_depth_observed": False,
                "fake_wall_threshold": FAKE_WALL_THRESHOLD,
                "memory_expiry_ms": MEMORY_EXPIRY_MS,
                "frames_with_wall_candidates": frames_with_wall_candidates,
                "degraded_flags": degraded_flags,
            },
        }

    def _new_observed_entry(
        self,
        symbol: str,
        side: str,
        price: float,
        volume: float,
        reference_time_local: datetime,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "side": side,
            "price": price,
            "last_observed_volume": volume,
            "max_observed_volume": volume,
            "first_seen_event_time": reference_time_local.isoformat(),
            "last_seen_event_time": reference_time_local.isoformat(),
            "carry_forward_volume": 0.0,
            "stale_age_ms": 0,
            "reappeared_count": 0,
            "executed_volume_at_price": 0.0,
            "disappeared_volume": 0.0,
            "trade_explained_ratio": 1.0,
            "cancel_inferred_volume": 0.0,
            "wall_suspicion_score": 0.0,
            "reconstruction_confidence": 0.95,
            "state": "OBSERVED_TOP3",
            "_baseline_disappeared_volume": 0.0,
            "_observed_this_frame": True,
        }

    def _wall_suspicion_score(
        self,
        unexplained_ratio: float,
        max_observed_volume: float,
        reappeared_count: int,
        stale_age_ms: int,
    ) -> float:
        volume_factor = clamp(max_observed_volume / WALL_SIZE_THRESHOLD)
        reappear_factor = clamp(reappeared_count / 3.0)
        stale_factor = clamp(stale_age_ms / MEMORY_EXPIRY_MS)
        return clamp(
            (0.35 * unexplained_ratio)
            + (0.20 * volume_factor)
            + (0.25 * reappear_factor)
            + (0.20 * stale_factor)
        )

    def _missing_state(
        self,
        stale_age_ms: int,
        trade_explained_ratio: float,
        carry_forward_volume: float,
        max_observed_volume: float,
        wall_suspicion_score: float,
        reappeared_count: int,
    ) -> str:
        if stale_age_ms >= MEMORY_EXPIRY_MS:
            return "EXPIRED"
        if trade_explained_ratio >= 0.95:
            return "CONSUMED_BY_TRADES"
        if carry_forward_volume > 0 and stale_age_ms <= CARRY_WINDOW_MS:
            return "CARRIED_OUTSIDE_TOP3"
        if (
            stale_age_ms >= FAKE_WALL_MIN_STALE_MS
            and reappeared_count > 0
            and wall_suspicion_score >= FAKE_WALL_THRESHOLD
            and max_observed_volume >= WALL_SIZE_THRESHOLD
        ):
            return "SUSPECTED_FAKE_WALL"
        return "PULLED_OR_CANCELLED"

    def _reconstruction_confidence(
        self,
        state: str,
        stale_age_ms: int,
        book_total_depth_unusable: bool,
        trade_side_nullable: bool,
    ) -> float:
        if state == "OBSERVED_TOP3":
            base = 0.95
        elif state == "REAPPEARED":
            base = 0.88
        elif state == "CONSUMED_BY_TRADES":
            base = 0.78
        elif state == "CARRIED_OUTSIDE_TOP3":
            base = 0.72
        elif state == "PULLED_OR_CANCELLED":
            base = 0.64
        elif state == "SUSPECTED_FAKE_WALL":
            base = 0.58
        else:
            base = 0.40

        base -= 0.20 * clamp(stale_age_ms / MEMORY_EXPIRY_MS)
        if book_total_depth_unusable:
            base -= 0.08
        if trade_side_nullable:
            base -= 0.04
        return clamp(base)

    def _log_event(self, symbol: str, trading_date: str, payload: dict[str, Any]) -> None:
        return

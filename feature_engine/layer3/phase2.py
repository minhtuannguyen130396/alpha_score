from __future__ import annotations

import math
import traceback
from bisect import bisect_right
from pathlib import Path
from typing import Any

from layer3.common import (
    CachedBundleFile,
    clamp,
    default_output_root,
    deterministic_write_json,
    find_cached_bundle,
    load_json_object,
    output_bundle_path,
    parse_iso_datetime,
    refresh_single_input_cache,
    safe_div,
    signed_trade_volume,
    simple_return,
    to_float,
    utc_now_iso,
)
from layer3.phase1 import BUNDLE_FOLDER as PHASE1_FOLDER


PHASE_NAME = "phase2_base_feature_engineering"
BUNDLE_TYPE = "base_feature_bundle"
BUNDLE_FOLDER = PHASE_NAME


def _index_slice(records: list[dict[str, Any]], index_range: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(index_range, dict):
        return []
    start_index = index_range.get("start_index")
    end_index = index_range.get("end_index")
    if not isinstance(start_index, int) or not isinstance(end_index, int):
        return []
    if start_index < 0 or end_index < start_index:
        return []
    return records[start_index : end_index + 1]


def _volume_sum(trades: list[dict[str, Any]]) -> float | None:
    values = [to_float(trade.get("volume")) for trade in trades if isinstance(trade, dict)]
    numeric = [value for value in values if value is not None]
    if not numeric:
        return None
    return sum(numeric)


def _signed_volume_sum(trades: list[dict[str, Any]]) -> float | None:
    values = [
        signed_trade_volume(trade.get("side"), trade.get("volume"))
        for trade in trades
        if isinstance(trade, dict)
    ]
    numeric = [value for value in values if value is not None]
    if not numeric:
        return None
    return sum(numeric)


def _realized_volatility(
    reference_timestamps: list[float],
    squared_return_prefix: list[float],
    valid_return_prefix: list[int],
    target_index: int,
    window_seconds: int,
) -> float | None:
    reference_timestamp = reference_timestamps[target_index]
    if reference_timestamp == float("-inf"):
        return None
    cutoff = reference_timestamp - window_seconds
    start_index = bisect_right(reference_timestamps, cutoff)
    lower_return_index = max(start_index + 1, 1)
    upper_return_index = target_index
    if upper_return_index < lower_return_index:
        return None
    valid_count = valid_return_prefix[upper_return_index + 1] - valid_return_prefix[lower_return_index]
    if valid_count <= 0:
        return None
    squared_sum = squared_return_prefix[upper_return_index + 1] - squared_return_prefix[lower_return_index]
    return math.sqrt(squared_sum)


class Phase2BaseFeatureEngineeringProcess:
    def __init__(
        self,
        output_root: Path | None = None,
        symbols: set[str] | None = None,
    ) -> None:
        self.output_root = Path(output_root) if output_root else default_output_root()
        self.symbols = {symbol.upper() for symbol in symbols} if symbols else None
        self._input_cache: dict[str, CachedBundleFile] = {}
        self._first_scan = True

    def run_once(self) -> dict[str, int]:
        dirty_items, self._first_scan = refresh_single_input_cache(
            self.output_root,
            PHASE1_FOLDER,
            self.symbols,
            self._input_cache,
            self._first_scan,
        )
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
                self._stdout(f"failed {PHASE_NAME} bundle for {symbol} {trading_date}: {exc}")
                self._stdout(traceback.format_exc().rstrip())

        if not dirty_items:
            self._stdout(f"{PHASE_NAME} cycle complete: idle")
        else:
            self._stdout(
                f"{PHASE_NAME} cycle complete: dirty={len(dirty_items)} built={built_count} removed={removed_count} failed={failed_count}"
            )

        return {
            "dirty": len(dirty_items),
            "built": built_count,
            "removed": removed_count,
            "failed": failed_count,
        }

    def _stdout(self, message: str) -> None:
        print(f"[{utc_now_iso()}] {message}")

    def _materialize_bundle(self, symbol: str, trading_date: str) -> str:
        bundle = self._build_bundle(symbol, trading_date)
        bundle_path = output_bundle_path(self.output_root, symbol, BUNDLE_FOLDER, trading_date)
        if bundle is None:
            if bundle_path.exists():
                bundle_path.unlink()
                self._stdout(f"removed stale {PHASE_NAME} bundle: {symbol} {trading_date}")
                return "removed"
            return "noop"

        deterministic_write_json(bundle_path, bundle)
        self._stdout(f"built {PHASE_NAME} bundle: {symbol} {trading_date}")
        return "built"

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        source_bundle = find_cached_bundle(self._input_cache, symbol, trading_date)
        if source_bundle is None:
            return None

        payload = source_bundle.payload
        phase0_path = Path(str(payload.get("source_bundle_path")))
        phase0_bundle = load_json_object(phase0_path)
        reference_frames = phase0_bundle.get("aligned_observed_branch", {}).get("reference_frames") or []
        trade_ticks = phase0_bundle.get("aligned_observed_branch", {}).get("aligned_sources", {}).get("trade_ticks") or []
        market_state_frames = payload.get("market_state_frames") or []
        if len(reference_frames) != len(market_state_frames):
            raise ValueError(f"phase1/phase0 frame mismatch for {symbol} {trading_date}")

        reference_times = [parse_iso_datetime(frame.get("reference_time_local")) for frame in market_state_frames]
        reference_timestamps = [moment.timestamp() if moment is not None else float("-inf") for moment in reference_times]
        current_prices = [to_float(frame.get("current_price")) for frame in market_state_frames]
        frame_returns: list[float | None] = [None]
        for frame_index in range(1, len(current_prices)):
            frame_returns.append(simple_return(current_prices[frame_index], current_prices[frame_index - 1]))

        squared_return_prefix = [0.0]
        valid_return_prefix = [0]
        for frame_return in frame_returns:
            squared_return_prefix.append(
                squared_return_prefix[-1] + ((frame_return or 0.0) ** 2 if frame_return is not None else 0.0)
            )
            valid_return_prefix.append(valid_return_prefix[-1] + (1 if frame_return is not None else 0))

        def lagged_return(frame_index: int, seconds_back: int) -> float | None:
            current_price = current_prices[frame_index]
            reference_time = reference_times[frame_index]
            if current_price in (None, 0.0) or reference_time is None:
                return None
            target_timestamp = reference_time.timestamp() - seconds_back
            history_index = bisect_right(reference_timestamps, target_timestamp) - 1
            if history_index < 0:
                return None
            previous_price = current_prices[history_index]
            return simple_return(current_price, previous_price)

        base_feature_frames: list[dict[str, Any]] = []
        for frame_index, (market_frame, reference_frame) in enumerate(zip(market_state_frames, reference_frames)):
            trade_windows = reference_frame.get("trade_window_index_ranges") or {}
            trades_10s = _index_slice(trade_ticks, trade_windows.get("10s"))
            trades_60s = _index_slice(trade_ticks, trade_windows.get("60s"))

            bid_depth_l1 = to_float(market_frame.get("bid_depth_l1"))
            bid_depth_l3 = to_float(market_frame.get("bid_depth_l3"))
            bid_depth_l5 = to_float(market_frame.get("bid_depth_l5"))
            ask_depth_l1 = to_float(market_frame.get("ask_depth_l1"))
            ask_depth_l3 = to_float(market_frame.get("ask_depth_l3"))
            ask_depth_l5 = to_float(market_frame.get("ask_depth_l5"))
            best_bid_size = to_float(market_frame.get("best_bid_size"))
            best_ask_size = to_float(market_frame.get("best_ask_size"))

            queue_pressure_l1 = None
            if best_bid_size is not None and best_ask_size is not None and (best_bid_size + best_ask_size) > 0:
                queue_pressure_l1 = best_bid_size / (best_bid_size + best_ask_size)

            base_feature_frames.append(
                {
                    "frame_no": market_frame.get("frame_no"),
                    "reference_time_local": market_frame.get("reference_time_local"),
                    "reference_time_utc": market_frame.get("reference_time_utc"),
                    "ret_1s": lagged_return(frame_index, 1),
                    "ret_5s": lagged_return(frame_index, 5),
                    "ret_30s": lagged_return(frame_index, 30),
                    "ret_60s": lagged_return(frame_index, 60),
                    "rv_10s": _realized_volatility(reference_timestamps, squared_return_prefix, valid_return_prefix, frame_index, 10),
                    "rv_60s": _realized_volatility(reference_timestamps, squared_return_prefix, valid_return_prefix, frame_index, 60),
                    "rv_5m": _realized_volatility(reference_timestamps, squared_return_prefix, valid_return_prefix, frame_index, 300),
                    "trade_count_10s": trade_windows.get("10s", {}).get("count"),
                    "trade_intensity_10s": safe_div(trade_windows.get("10s", {}).get("count"), 10.0),
                    "volume_sum_10s": _volume_sum(trades_10s),
                    "signed_volume_10s": _signed_volume_sum(trades_10s),
                    "signed_volume_60s": _signed_volume_sum(trades_60s),
                    "book_imbalance_l1": safe_div(
                        (bid_depth_l1 or 0.0) - (ask_depth_l1 or 0.0),
                        (bid_depth_l1 or 0.0) + (ask_depth_l1 or 0.0),
                    ),
                    "book_imbalance_l3": safe_div(
                        (bid_depth_l3 or 0.0) - (ask_depth_l3 or 0.0),
                        (bid_depth_l3 or 0.0) + (ask_depth_l3 or 0.0),
                    ),
                    "book_imbalance_l5": safe_div(
                        (bid_depth_l5 or 0.0) - (ask_depth_l5 or 0.0),
                        (bid_depth_l5 or 0.0) + (ask_depth_l5 or 0.0),
                    ),
                    "depth_ratio_l1": safe_div(bid_depth_l1, ask_depth_l1),
                    "depth_ratio_l3": safe_div(bid_depth_l3, ask_depth_l3),
                    "depth_ratio_l5": safe_div(bid_depth_l5, ask_depth_l5),
                    "queue_pressure_l1": queue_pressure_l1,
                    "trade_window_counts": {
                        window_name: window_range.get("count")
                        for window_name, window_range in trade_windows.items()
                        if isinstance(window_range, dict)
                    },
                }
            )

        first_frame = base_feature_frames[0] if base_feature_frames else {}
        last_frame = base_feature_frames[-1] if base_feature_frames else {}
        return {
            "bundle_type": BUNDLE_TYPE,
            "phase": PHASE_NAME,
            "symbol": payload.get("symbol"),
            "trading_date": payload.get("trading_date"),
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "source_layer3_input_bundle_path": str(phase0_path),
            "validation_status": payload.get("validation_status"),
            "degraded_flags": payload.get("degraded_flags") or {},
            "formula_notes": {
                "returns": "current_price / lagged_price - 1 using the latest frame at or before reference_time - window",
                "rv_windows": "sqrt(sum(simple_return^2)) over frame-to-frame returns inside the trailing window",
                "signed_volume": "B adds volume, S subtracts volume, null side contributes 0",
                "queue_pressure_l1": "best_bid_size / (best_bid_size + best_ask_size)",
            },
            "frame_count": len(base_feature_frames),
            "reference_time_summary": {
                "first_reference_time_local": first_frame.get("reference_time_local"),
                "last_reference_time_local": last_frame.get("reference_time_local"),
            },
            "market_state_frames": market_state_frames,
            "base_feature_frames": base_feature_frames,
        }


__all__ = [
    "BUNDLE_FOLDER",
    "BUNDLE_TYPE",
    "PHASE_NAME",
    "Phase2BaseFeatureEngineeringProcess",
]

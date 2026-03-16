from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

from layer3.common import (
    CachedBundleFile,
    clamp,
    default_output_root,
    deterministic_write_json,
    find_cached_bundle,
    is_auction_time,
    is_near_close_time,
    is_session_time,
    load_json_object,
    output_bundle_path,
    parse_iso_datetime,
    refresh_single_input_cache,
    utc_now_iso,
)
from layer3.phase3 import BUNDLE_FOLDER as PHASE3_FOLDER


PHASE_NAME = "phase4_feature_quality"
BUNDLE_TYPE = "feature_quality_bundle"
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


def _freshness_coverage(stale_ms: Any, window_ms: int, has_events: bool = True) -> float:
    if not has_events:
        return 0.0
    if stale_ms is None:
        return 0.0
    return clamp(1.0 - (float(stale_ms) / float(window_ms)))


class Phase4FeatureQualityProcess:
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
            PHASE3_FOLDER,
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
        market_state_frames = payload.get("market_state_frames") or []
        base_feature_frames = payload.get("base_feature_frames") or []
        feature_store_frames = payload.get("feature_store_frames") or []
        if not (len(market_state_frames) == len(base_feature_frames) == len(feature_store_frames)):
            raise ValueError(f"phase3 frame mismatch for {symbol} {trading_date}")

        phase0_path = Path(str(payload.get("source_layer3_input_bundle_path")))
        phase0_bundle = load_json_object(phase0_path)
        reference_frames = phase0_bundle.get("aligned_observed_branch", {}).get("reference_frames") or []
        trade_ticks = phase0_bundle.get("aligned_observed_branch", {}).get("aligned_sources", {}).get("trade_ticks") or []
        if len(reference_frames) != len(market_state_frames):
            raise ValueError(f"phase0/phase3 frame mismatch for {symbol} {trading_date}")

        validation_status = str(payload.get("validation_status") or "").lower()
        degraded_flags = payload.get("degraded_flags") or {}
        duplicate_times = {
            value
            for value in [frame.get("reference_time_local") for frame in market_state_frames]
            if value is not None and [other.get("reference_time_local") for other in market_state_frames].count(value) > 1
        }
        available_pre_session_days = payload.get("daily_history_metadata", {}).get("available_pre_session_days")

        feature_quality_frames: list[dict[str, Any]] = []
        for market_frame, base_frame, feature_frame, reference_frame in zip(
            market_state_frames,
            base_feature_frames,
            feature_store_frames,
            reference_frames,
        ):
            reference_time = parse_iso_datetime(market_frame.get("reference_time_local"))
            stale_tick_ms = market_frame.get("tick_age_ms")
            stale_book_ms = market_frame.get("book_age_ms")
            trade_count_10s = base_frame.get("trade_count_10s") or 0
            trade_count_60s = base_frame.get("trade_window_counts", {}).get("60s") or 0

            tick_coverage_10s = _freshness_coverage(stale_tick_ms, 10_000, bool(trade_count_10s))
            tick_coverage_60s = _freshness_coverage(stale_tick_ms, 60_000, bool(trade_count_60s))
            book_coverage_10s = _freshness_coverage(stale_book_ms, 10_000, True)
            book_coverage_60s = _freshness_coverage(stale_book_ms, 60_000, True)

            trades_60s = _index_slice(trade_ticks, reference_frame.get("trade_window_index_ranges", {}).get("60s"))
            null_side_count = sum(1 for trade in trades_60s if trade.get("side") is None)
            trade_side_inferred_ratio = null_side_count / len(trades_60s) if trades_60s else 0.0

            spread = market_frame.get("spread")
            spread_bps = market_frame.get("spread_bps")
            ret_1s = base_frame.get("ret_1s")
            ret_5s = base_frame.get("ret_5s")
            rv_10s = base_frame.get("rv_10s")

            spread_invalid_flag = spread is None or spread < 0
            outlier_flag = any(
                condition
                for condition in (
                    ret_1s is not None and abs(ret_1s) > 0.03,
                    ret_5s is not None and abs(ret_5s) > 0.06,
                    rv_10s is not None and rv_10s > 0.08,
                    spread_bps is not None and spread_bps > 500.0,
                )
            )

            book_reconstructed_flag = bool(market_frame.get("used_reconstructed_depth_l5"))
            missing_depth_level_count = int(market_frame.get("missing_depth_level_count") or 0)
            duplicate_event_flag = market_frame.get("reference_time_local") in duplicate_times
            session_valid_flag = is_session_time(reference_time)
            auction_flag = is_auction_time(reference_time)
            near_close_flag = is_near_close_time(reference_time)

            confidence = 1.0
            if validation_status == "fail":
                confidence -= 0.35
            elif validation_status == "degraded":
                confidence -= 0.15
            confidence -= 0.10 * min(sum(1 for value in degraded_flags.values() if value), 3)
            confidence -= 0.20 * (1.0 - tick_coverage_60s)
            confidence -= 0.20 * (1.0 - book_coverage_60s)
            confidence -= 0.10 * trade_side_inferred_ratio
            confidence -= 0.08 if book_reconstructed_flag else 0.0
            confidence -= min(missing_depth_level_count, 4) * 0.03
            confidence -= 0.08 if spread_invalid_flag else 0.0
            confidence -= 0.05 if outlier_flag else 0.0
            confidence -= 0.05 if duplicate_event_flag else 0.0
            confidence -= 0.05 if not session_valid_flag else 0.0
            feature_confidence = clamp(confidence)

            data_quality_score = clamp(
                (
                    feature_confidence
                    + tick_coverage_10s
                    + tick_coverage_60s
                    + book_coverage_10s
                    + book_coverage_60s
                    + (1.0 if session_valid_flag else 0.0)
                )
                / 6.0
            )

            feature_quality_frames.append(
                {
                    "frame_no": market_frame.get("frame_no"),
                    "reference_time_local": market_frame.get("reference_time_local"),
                    "reference_time_utc": market_frame.get("reference_time_utc"),
                    "tick_coverage_10s": tick_coverage_10s,
                    "tick_coverage_60s": tick_coverage_60s,
                    "book_coverage_10s": book_coverage_10s,
                    "book_coverage_60s": book_coverage_60s,
                    "daily_history_available_days": available_pre_session_days,
                    "stale_tick_ms": stale_tick_ms,
                    "stale_book_ms": stale_book_ms,
                    "stale_book_seconds": (stale_book_ms / 1000.0) if stale_book_ms is not None else None,
                    "trade_side_inferred_ratio": trade_side_inferred_ratio,
                    "book_reconstructed_flag": book_reconstructed_flag,
                    "missing_depth_level_count": missing_depth_level_count,
                    "outlier_flag": outlier_flag,
                    "spread_invalid_flag": spread_invalid_flag,
                    "duplicate_event_flag": duplicate_event_flag,
                    "session_valid_flag": session_valid_flag,
                    "auction_flag": auction_flag,
                    "near_close_flag": near_close_flag,
                    "feature_confidence": feature_confidence,
                    "data_quality_score": data_quality_score,
                }
            )

        first_frame = feature_quality_frames[0] if feature_quality_frames else {}
        last_frame = feature_quality_frames[-1] if feature_quality_frames else {}
        return {
            "bundle_type": BUNDLE_TYPE,
            "phase": PHASE_NAME,
            "symbol": payload.get("symbol"),
            "trading_date": payload.get("trading_date"),
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "source_layer3_input_bundle_path": payload.get("source_layer3_input_bundle_path"),
            "formula_notes": {
                "tick_coverage": "scaled freshness score inside the trailing window; 1 means fresh updates close to reference_time",
                "book_reconstructed_flag": "true when depth_l5 depends on reconstructed phase4 memory beyond observed top3",
                "trade_side_inferred_ratio": "share of trades with null side in the 60s trailing window",
                "feature_confidence": "confidence score after penalties for stale data, degraded validation, reconstruction use, and outliers",
            },
            "frame_count": len(feature_quality_frames),
            "reference_time_summary": {
                "first_reference_time_local": first_frame.get("reference_time_local"),
                "last_reference_time_local": last_frame.get("reference_time_local"),
            },
            "market_state_frames": market_state_frames,
            "base_feature_frames": base_feature_frames,
            "feature_store_frames": feature_store_frames,
            "feature_quality_frames": feature_quality_frames,
        }


__all__ = [
    "BUNDLE_FOLDER",
    "BUNDLE_TYPE",
    "PHASE_NAME",
    "Phase4FeatureQualityProcess",
]

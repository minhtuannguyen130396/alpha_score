from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

from layer3.common import (
    CachedBundleFile,
    coerce_date_text,
    default_input_root,
    default_output_root,
    deterministic_write_json,
    find_cached_bundle,
    output_bundle_path,
    refresh_single_input_cache,
    sanitize_symbol,
    utc_now_iso,
)


PHASE_NAME = "phase0_layer2_input_intake"
BUNDLE_TYPE = "layer3_input_bundle"
BUNDLE_FOLDER = PHASE_NAME
PHASE2_FOLDER = "phase2_input_validation"
PHASE3_FOLDER = "phase3_time_alignment_reference_indexing"
PHASE4_FOLDER = "phase4_book_memory_deep_reconstruction"


def _strip_ignored_fields(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, nested_value in value.items():
            if key == "matched_price":
                continue
            cleaned[key] = _strip_ignored_fields(nested_value)
        return cleaned
    if isinstance(value, list):
        return [_strip_ignored_fields(item) for item in value]
    return value


class Phase0Layer2InputIntakeProcess:
    def __init__(
        self,
        input_root: Path | None = None,
        output_root: Path | None = None,
        symbols: set[str] | None = None,
    ) -> None:
        self.input_root = Path(input_root) if input_root else default_input_root()
        self.output_root = Path(output_root) if output_root else default_output_root()
        self.symbols = {symbol.upper() for symbol in symbols} if symbols else None
        self._phase2_cache: dict[str, CachedBundleFile] = {}
        self._phase3_cache: dict[str, CachedBundleFile] = {}
        self._phase4_cache: dict[str, CachedBundleFile] = {}
        self._phase2_first_scan = True
        self._phase3_first_scan = True
        self._phase4_first_scan = True

    def run_once(self) -> dict[str, int]:
        dirty_items = self._refresh_input_caches()
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

    def _refresh_input_caches(self) -> set[tuple[str, str]]:
        dirty_items: set[tuple[str, str]] = set()

        phase2_dirty, self._phase2_first_scan = refresh_single_input_cache(
            self.input_root,
            PHASE2_FOLDER,
            self.symbols,
            self._phase2_cache,
            self._phase2_first_scan,
        )
        phase3_dirty, self._phase3_first_scan = refresh_single_input_cache(
            self.input_root,
            PHASE3_FOLDER,
            self.symbols,
            self._phase3_cache,
            self._phase3_first_scan,
        )
        phase4_dirty, self._phase4_first_scan = refresh_single_input_cache(
            self.input_root,
            PHASE4_FOLDER,
            self.symbols,
            self._phase4_cache,
            self._phase4_first_scan,
        )
        dirty_items.update(phase2_dirty)
        dirty_items.update(phase3_dirty)
        dirty_items.update(phase4_dirty)
        return dirty_items

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
        phase2_bundle = find_cached_bundle(self._phase2_cache, symbol, trading_date)
        phase3_bundle = find_cached_bundle(self._phase3_cache, symbol, trading_date)
        phase4_bundle = find_cached_bundle(self._phase4_cache, symbol, trading_date)

        if phase2_bundle is None or phase3_bundle is None or phase4_bundle is None:
            return None

        payload2 = phase2_bundle.payload
        payload3 = phase3_bundle.payload
        payload4 = phase4_bundle.payload
        self._validate_dependencies(symbol, trading_date, payload2, payload3, payload4)

        reference_frames = _strip_ignored_fields(payload3.get("reference_frames") or [])
        deep_book_frames = _strip_ignored_fields(payload4.get("deep_book_frames") or [])
        dependency_report = {
            "symbol_match": True,
            "trading_date_match": True,
            "reference_frame_count": len(reference_frames),
            "deep_book_frame_count": len(deep_book_frames),
            "reference_time_mismatch_count": 0,
            "status": "pass",
        }

        daily_context = _strip_ignored_fields(payload3.get("aligned_sources", {}).get("daily_context"))
        normalized_daily_input = _strip_ignored_fields(payload2.get("normalized_daily_input"))
        aligned_sources = _strip_ignored_fields(payload3.get("aligned_sources") or {})
        latest_memory_levels = _strip_ignored_fields(payload4.get("latest_memory_levels") or [])
        reconstruction_quality = _strip_ignored_fields(payload4.get("reconstruction_quality") or {})
        degraded_flags = _strip_ignored_fields(payload3.get("degraded_flags") or payload2.get("degraded_flags") or {})
        session_filter_local = _strip_ignored_fields(payload2.get("session_filter_local") or {})
        window_definitions_seconds = _strip_ignored_fields(payload3.get("window_definitions_seconds") or {})

        return {
            "bundle_type": BUNDLE_TYPE,
            "phase": PHASE_NAME,
            "symbol": sanitize_symbol(symbol),
            "trading_date": trading_date,
            "generated_at_utc": utc_now_iso(),
            "source_bundle_paths": {
                "validated_input_bundle": str(phase2_bundle.path),
                "aligned_input_frame": str(phase3_bundle.path),
                "deep_book_input_bundle": str(phase4_bundle.path),
            },
            "source_bundle_generated_at_utc": {
                "validated_input_bundle": payload2.get("generated_at_utc"),
                "aligned_input_frame": payload3.get("generated_at_utc"),
                "deep_book_input_bundle": payload4.get("generated_at_utc"),
            },
            "dependency_report": dependency_report,
            "validation_status": payload3.get("validation_status") or payload2.get("input_quality", {}).get("validation_status"),
            "degraded_flags": degraded_flags,
            "session_filter_local": session_filter_local,
            "window_definitions_seconds": window_definitions_seconds,
            "daily_branch": {
                "normalized_daily_input": normalized_daily_input,
                "daily_context": daily_context,
            },
            "aligned_observed_branch": {
                "reference_axis_source": payload3.get("reference_axis_source"),
                "reference_frame_count": payload3.get("reference_frame_count"),
                "reference_time_summary": payload3.get("reference_time_summary"),
                "aligned_sources": aligned_sources,
                "reference_frames": reference_frames,
            },
            "deep_book_branch": {
                "frame_count": payload4.get("frame_count"),
                "deep_book_frames": deep_book_frames,
                "latest_memory_levels": latest_memory_levels,
                "reconstruction_quality": reconstruction_quality,
            },
        }

    def _validate_dependencies(
        self,
        symbol: str,
        trading_date: str,
        payload2: dict[str, Any],
        payload3: dict[str, Any],
        payload4: dict[str, Any],
    ) -> None:
        expected_symbol = sanitize_symbol(symbol)
        expected_date = coerce_date_text(trading_date)
        for payload in (payload2, payload3, payload4):
            payload_symbol = sanitize_symbol(str(payload.get("symbol") or expected_symbol))
            payload_date = coerce_date_text(payload.get("trading_date")) or expected_date
            if payload_symbol != expected_symbol:
                raise ValueError(f"symbol mismatch across layer2 inputs for {symbol} {trading_date}")
            if payload_date != expected_date:
                raise ValueError(f"trading_date mismatch across layer2 inputs for {symbol} {trading_date}")

        reference_frames = payload3.get("reference_frames") or []
        deep_book_frames = payload4.get("deep_book_frames") or []
        if len(reference_frames) != len(deep_book_frames):
            raise ValueError(
                f"frame count mismatch for {symbol} {trading_date}: phase3={len(reference_frames)} phase4={len(deep_book_frames)}"
            )

        for index, (reference_frame, deep_frame) in enumerate(zip(reference_frames, deep_book_frames), start=1):
            if reference_frame.get("frame_no") != deep_frame.get("frame_no"):
                raise ValueError(f"frame_no mismatch at frame {index} for {symbol} {trading_date}")
            if reference_frame.get("reference_time_local") != deep_frame.get("reference_time_local"):
                raise ValueError(f"reference_time_local mismatch at frame {index} for {symbol} {trading_date}")
            if reference_frame.get("reference_time_utc") != deep_frame.get("reference_time_utc"):
                raise ValueError(f"reference_time_utc mismatch at frame {index} for {symbol} {trading_date}")


__all__ = [
    "BUNDLE_FOLDER",
    "BUNDLE_TYPE",
    "PHASE_NAME",
    "Phase0Layer2InputIntakeProcess",
    "default_input_root",
    "default_output_root",
    "utc_now_iso",
]

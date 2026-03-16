from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

from layer3.common import (
    CachedBundleFile,
    default_output_root,
    deterministic_write_json,
    find_cached_bundle,
    output_bundle_path,
    refresh_single_input_cache,
    utc_now_iso,
)
from layer3.phase4 import BUNDLE_FOLDER as PHASE4_FOLDER


PHASE_NAME = "phase5_layer3_output_package"
BUNDLE_TYPE = "layer3_output_package"
BUNDLE_FOLDER = PHASE_NAME


class Phase5Layer3OutputPackageProcess:
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
            PHASE4_FOLDER,
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
        feature_quality_frames = payload.get("feature_quality_frames") or []
        if not (
            len(market_state_frames)
            == len(base_feature_frames)
            == len(feature_store_frames)
            == len(feature_quality_frames)
        ):
            raise ValueError(f"phase4 frame mismatch for {symbol} {trading_date}")

        final_frames: list[dict[str, Any]] = []
        for market_state, base_feature, feature_store, feature_quality in zip(
            market_state_frames,
            base_feature_frames,
            feature_store_frames,
            feature_quality_frames,
        ):
            merged_feature_store = dict(base_feature)
            merged_feature_store.update(feature_store)
            merged_feature_store.pop("frame_no", None)
            merged_feature_store.pop("reference_time_local", None)
            merged_feature_store.pop("reference_time_utc", None)

            final_frames.append(
                {
                    "frame_no": market_state.get("frame_no"),
                    "reference_time_local": market_state.get("reference_time_local"),
                    "reference_time_utc": market_state.get("reference_time_utc"),
                    "symbol": payload.get("symbol"),
                    "trading_date": payload.get("trading_date"),
                    "source_bundle_paths": {
                        "phase4_feature_quality_bundle": str(source_bundle.path),
                        "phase3_feature_store_bundle": payload.get("source_bundle_path"),
                        "layer3_input_bundle": payload.get("source_layer3_input_bundle_path"),
                    },
                    "market_state": market_state,
                    "feature_store": merged_feature_store,
                    "feature_quality": feature_quality,
                }
            )

        first_frame = final_frames[0] if final_frames else {}
        last_frame = final_frames[-1] if final_frames else {}
        return {
            "bundle_type": BUNDLE_TYPE,
            "phase": PHASE_NAME,
            "symbol": payload.get("symbol"),
            "trading_date": payload.get("trading_date"),
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(source_bundle.path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "frame_count": len(final_frames),
            "reference_time_summary": {
                "first_reference_time_local": first_frame.get("reference_time_local"),
                "last_reference_time_local": last_frame.get("reference_time_local"),
            },
            "frames": final_frames,
        }


__all__ = [
    "BUNDLE_FOLDER",
    "BUNDLE_TYPE",
    "PHASE_NAME",
    "Phase5Layer3OutputPackageProcess",
]

from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

from layer3.common import (
    CachedBundleFile,
    clamp,
    coerce_date_text,
    default_output_root,
    deterministic_write_json,
    find_cached_bundle,
    load_json_object,
    output_bundle_path,
    parse_iso_datetime,
    price_token,
    refresh_single_input_cache,
    safe_div,
    sanitize_symbol,
    to_float,
    utc_now_iso,
)
from layer3.phase0 import BUNDLE_FOLDER as PHASE0_FOLDER


PHASE_NAME = "phase1_market_state_construction"
BUNDLE_TYPE = "market_state_bundle"
BUNDLE_FOLDER = PHASE_NAME


def _normalized_levels(snapshot: dict[str, Any] | None, side: str) -> list[dict[str, float]]:
    if not isinstance(snapshot, dict):
        return []
    field_name = "bid_levels" if side == "bid" else "ask_levels"
    levels: list[dict[str, float]] = []
    for raw_level in snapshot.get(field_name) or []:
        if not isinstance(raw_level, dict):
            continue
        price = price_token(raw_level.get("price"))
        volume = to_float(raw_level.get("volume"))
        if price is None or volume is None:
            continue
        levels.append({"price": price, "volume": volume})
    return levels


def _level_depth(levels: list[dict[str, float]], limit: int) -> float:
    return sum(level["volume"] for level in levels[:limit])


def _memory_l5_extension(
    observed_levels: list[dict[str, float]],
    deep_frame: dict[str, Any] | None,
    side: str,
) -> tuple[float, int, bool, list[dict[str, float]]]:
    observed_prices = {level["price"] for level in observed_levels}
    chosen = list(observed_levels[:3])
    extra_levels: list[dict[str, float]] = []

    if isinstance(deep_frame, dict):
        memory_levels = deep_frame.get("memory_levels") or []
        candidates: list[dict[str, float]] = []
        for entry in memory_levels:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("side") or "").lower() != side:
                continue
            price = price_token(entry.get("price"))
            if price is None or price in observed_prices:
                continue
            if str(entry.get("state") or "").upper() == "EXPIRED":
                continue
            carry_volume = to_float(entry.get("carry_forward_volume"))
            last_volume = to_float(entry.get("last_observed_volume"))
            candidate_volume = carry_volume if carry_volume and carry_volume > 0 else last_volume
            if candidate_volume is None or candidate_volume <= 0:
                continue
            candidates.append({"price": price, "volume": candidate_volume})

        candidates.sort(key=lambda item: item["price"], reverse=side == "bid")
        for candidate in candidates:
            if len(chosen) >= 5:
                break
            chosen.append(candidate)
            extra_levels.append(candidate)

    depth = sum(level["volume"] for level in chosen[:5])
    missing_levels = max(0, 5 - len(chosen[:5]))
    used_reconstructed = bool(extra_levels)
    return depth, missing_levels, used_reconstructed, extra_levels


def _cumulative_trade_value(trades: list[dict[str, Any]], end_index: int | None) -> float | None:
    if end_index is None or end_index < 0:
        return None
    total = 0.0
    has_value = False
    for trade in trades[: end_index + 1]:
        price = to_float(trade.get("price"))
        volume = to_float(trade.get("volume"))
        if price is None or volume is None:
            continue
        total += price * volume
        has_value = True
    return total if has_value else None


class Phase1MarketStateConstructionProcess:
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
            PHASE0_FOLDER,
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
        phase0_path = Path(str(source_bundle.path))
        aligned_branch = payload.get("aligned_observed_branch") or {}
        deep_branch = payload.get("deep_book_branch") or {}
        aligned_sources = aligned_branch.get("aligned_sources") or {}
        reference_frames = aligned_branch.get("reference_frames") or []
        deep_frames = deep_branch.get("deep_book_frames") or []
        intraday_snapshots = aligned_sources.get("intraday_snapshots") or []
        orderbook_snapshots = aligned_sources.get("orderbook_snapshots") or []
        trade_ticks = aligned_sources.get("trade_ticks") or []
        daily_context = (
            payload.get("daily_branch", {}).get("daily_context")
            or payload.get("daily_branch", {}).get("normalized_daily_input")
            or {}
        )

        if len(reference_frames) != len(deep_frames):
            raise ValueError(f"phase0 reference/deep frame mismatch for {symbol} {trading_date}")

        day_open = to_float(daily_context.get("price_open"))
        running_high: float | None = None
        running_low: float | None = None
        market_state_frames: list[dict[str, Any]] = []

        for frame_index, reference_frame in enumerate(reference_frames):
            intraday_index = reference_frame.get("intraday_snapshot_index")
            orderbook_index = reference_frame.get("orderbook_snapshot_index")
            last_trade_index = reference_frame.get("last_trade_index")
            intraday_snapshot = (
                intraday_snapshots[intraday_index]
                if isinstance(intraday_index, int) and 0 <= intraday_index < len(intraday_snapshots)
                else None
            )
            orderbook_snapshot = (
                orderbook_snapshots[orderbook_index]
                if isinstance(orderbook_index, int) and 0 <= orderbook_index < len(orderbook_snapshots)
                else None
            )
            last_trade = (
                trade_ticks[last_trade_index]
                if isinstance(last_trade_index, int) and 0 <= last_trade_index < len(trade_ticks)
                else None
            )
            deep_frame = deep_frames[frame_index] if frame_index < len(deep_frames) else None

            bid_levels = _normalized_levels(orderbook_snapshot, "bid")
            ask_levels = _normalized_levels(orderbook_snapshot, "ask")
            best_bid = bid_levels[0]["price"] if bid_levels else None
            best_bid_size = bid_levels[0]["volume"] if bid_levels else None
            best_ask = ask_levels[0]["price"] if ask_levels else None
            best_ask_size = ask_levels[0]["volume"] if ask_levels else None

            bid_depth_l1 = _level_depth(bid_levels, 1)
            bid_depth_l3 = _level_depth(bid_levels, 3)
            ask_depth_l1 = _level_depth(ask_levels, 1)
            ask_depth_l3 = _level_depth(ask_levels, 3)
            bid_depth_l5, bid_missing_levels, bid_used_reconstructed, bid_extra_levels = _memory_l5_extension(
                bid_levels,
                deep_frame,
                "bid",
            )
            ask_depth_l5, ask_missing_levels, ask_used_reconstructed, ask_extra_levels = _memory_l5_extension(
                ask_levels,
                deep_frame,
                "ask",
            )

            last_trade_price = to_float(last_trade.get("price")) if isinstance(last_trade, dict) else None
            last_trade_volume = to_float(last_trade.get("volume")) if isinstance(last_trade, dict) else None
            last_trade_side = last_trade.get("side") if isinstance(last_trade, dict) else None

            cum_volume = to_float(intraday_snapshot.get("deal_volume")) if isinstance(intraday_snapshot, dict) else None
            cum_trade_value = to_float(intraday_snapshot.get("deal_value")) if isinstance(intraday_snapshot, dict) else None
            if cum_trade_value is None:
                cum_trade_value = _cumulative_trade_value(trade_ticks, last_trade_index if isinstance(last_trade_index, int) else None)
            if cum_volume is None and isinstance(last_trade_index, int) and last_trade_index >= 0:
                cum_volume = sum(
                    to_float(trade.get("volume")) or 0.0
                    for trade in trade_ticks[: last_trade_index + 1]
                    if isinstance(trade, dict)
                )
            trade_count_intraday = last_trade_index + 1 if isinstance(last_trade_index, int) and last_trade_index >= 0 else 0

            current_price = None
            if isinstance(intraday_snapshot, dict):
                current_price = to_float(intraday_snapshot.get("last_price"))
            if current_price is None:
                current_price = last_trade_price

            if current_price is not None:
                running_high = current_price if running_high is None else max(running_high, current_price)
                running_low = current_price if running_low is None else min(running_low, current_price)

            mid_price = None
            if best_bid is not None and best_ask is not None:
                mid_price = (best_bid + best_ask) / 2.0

            spread = None
            spread_bps = None
            if best_bid is not None and best_ask is not None:
                spread = best_ask - best_bid
                if mid_price not in (None, 0.0):
                    spread_bps = spread / mid_price * 10_000.0

            vwap_intraday = safe_div(cum_trade_value, cum_volume)
            intraday_return_from_open = None
            if current_price is not None and day_open not in (None, 0.0):
                intraday_return_from_open = current_price / day_open - 1.0

            intraday_pos_in_range = None
            if (
                current_price is not None
                and running_high is not None
                and running_low is not None
                and running_high > running_low
            ):
                intraday_pos_in_range = clamp((current_price - running_low) / (running_high - running_low))

            age_metadata = reference_frame.get("age_metadata") or {}
            market_state_frames.append(
                {
                    "frame_no": reference_frame.get("frame_no"),
                    "reference_time_local": reference_frame.get("reference_time_local"),
                    "reference_time_utc": reference_frame.get("reference_time_utc"),
                    "source_indices": {
                        "intraday_snapshot_index": intraday_index,
                        "orderbook_snapshot_index": orderbook_index,
                        "last_trade_index": last_trade_index,
                    },
                    "last_trade_price": last_trade_price,
                    "last_trade_volume": last_trade_volume,
                    "last_trade_side": last_trade_side,
                    "cum_volume": cum_volume,
                    "trade_count_intraday": trade_count_intraday,
                    "best_bid": best_bid,
                    "best_bid_size": best_bid_size,
                    "best_ask": best_ask,
                    "best_ask_size": best_ask_size,
                    "mid_price": mid_price,
                    "spread": spread,
                    "spread_bps": spread_bps,
                    "bid_depth_l1": bid_depth_l1,
                    "bid_depth_l3": bid_depth_l3,
                    "bid_depth_l5": bid_depth_l5,
                    "ask_depth_l1": ask_depth_l1,
                    "ask_depth_l3": ask_depth_l3,
                    "ask_depth_l5": ask_depth_l5,
                    "day_open": day_open,
                    "day_high_so_far": running_high,
                    "day_low_so_far": running_low,
                    "current_price": current_price,
                    "vwap_intraday": vwap_intraday,
                    "intraday_return_from_open": intraday_return_from_open,
                    "intraday_pos_in_range": intraday_pos_in_range,
                    "book_age_ms": age_metadata.get("book_age_ms"),
                    "tick_age_ms": age_metadata.get("tick_age_ms"),
                    "cum_trade_value": cum_trade_value,
                    "used_reconstructed_depth_l5": bid_used_reconstructed or ask_used_reconstructed,
                    "missing_depth_level_count": bid_missing_levels + ask_missing_levels,
                    "observed_bid_level_count": len(bid_levels),
                    "observed_ask_level_count": len(ask_levels),
                    "reconstructed_bid_levels_used": bid_extra_levels,
                    "reconstructed_ask_levels_used": ask_extra_levels,
                    "source_snapshot_times": {
                        "intraday_event_time_local": intraday_snapshot.get("event_time_local") if isinstance(intraday_snapshot, dict) else None,
                        "orderbook_event_time_local": orderbook_snapshot.get("event_time_local") if isinstance(orderbook_snapshot, dict) else None,
                        "last_trade_event_time_local": last_trade.get("event_time_local") if isinstance(last_trade, dict) else None,
                    },
                }
            )

        first_frame = market_state_frames[0] if market_state_frames else {}
        last_frame = market_state_frames[-1] if market_state_frames else {}
        return {
            "bundle_type": BUNDLE_TYPE,
            "phase": PHASE_NAME,
            "symbol": sanitize_symbol(symbol),
            "trading_date": coerce_date_text(trading_date) or trading_date,
            "generated_at_utc": utc_now_iso(),
            "source_bundle_path": str(phase0_path),
            "source_bundle_generated_at_utc": payload.get("generated_at_utc"),
            "validation_status": payload.get("validation_status"),
            "degraded_flags": payload.get("degraded_flags") or {},
            "formula_notes": {
                "spread": "best_ask - best_bid",
                "spread_bps": "spread / mid_price * 10000 when mid_price > 0",
                "mid_price": "(best_bid + best_ask) / 2 when both sides exist",
                "vwap_intraday": "cum_trade_value / cum_volume when cum_volume > 0",
                "intraday_return_from_open": "current_price / day_open - 1",
                "intraday_pos_in_range": "(current_price - day_low_so_far) / (day_high_so_far - day_low_so_far)",
                "depth_l5": "observed top3 plus up to two reconstructed levels from phase4 memory",
            },
            "frame_count": len(market_state_frames),
            "reference_time_summary": {
                "first_reference_time_local": first_frame.get("reference_time_local"),
                "last_reference_time_local": last_frame.get("reference_time_local"),
            },
            "market_state_frames": market_state_frames,
        }


__all__ = [
    "BUNDLE_FOLDER",
    "BUNDLE_TYPE",
    "PHASE_NAME",
    "Phase1MarketStateConstructionProcess",
]

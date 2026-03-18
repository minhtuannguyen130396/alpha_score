from __future__ import annotations

import traceback
from datetime import datetime
from datetime import time as time_value
from pathlib import Path
from statistics import fmean
from typing import Any

from layer3.common import (
    CachedBundleFile,
    default_daily_history_root,
    default_output_root,
    deterministic_write_json,
    find_cached_bundle,
    is_near_close_time,
    load_json_records,
    output_bundle_path,
    parse_iso_datetime,
    price_token,
    refresh_single_input_cache,
    safe_div,
    simple_return,
    stddev_or_none,
    to_float,
    utc_now_iso,
)
from layer3.phase2 import BUNDLE_FOLDER as PHASE2_FOLDER


PHASE_NAME = "phase3_feature_store"
BUNDLE_TYPE = "feature_store_bundle"
BUNDLE_FOLDER = PHASE_NAME


def _normalized_daily_bar(record: dict[str, Any]) -> dict[str, Any] | None:
    date_text = str(record.get("date") or "").strip()[:10]
    if len(date_text) != 10:
        return None
    return {
        "date": date_text,
        "open": to_float(record.get("priceOpen")),
        "high": to_float(record.get("priceHigh")),
        "low": to_float(record.get("priceLow")),
        "close": to_float(record.get("priceClose")),
        "volume": to_float(record.get("totalVolume")),
        "buy_foreign_quantity": to_float(record.get("buyForeignQuantity")),
        "sell_foreign_quantity": to_float(record.get("sellForeignQuantity")),
        "total_value": to_float(record.get("totalValue")),
    }


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return fmean(values[-period:])


def _rolling_sma(values: list[float], period: int) -> list[float | None]:
    results: list[float | None] = []
    for index in range(len(values)):
        if index + 1 < period:
            results.append(None)
        else:
            results.append(fmean(values[index + 1 - period : index + 1]))
    return results


def _ema_series(values: list[float], period: int) -> list[float | None]:
    results: list[float | None] = []
    if len(values) < period:
        return [None for _ in values]
    multiplier = 2.0 / (period + 1.0)
    seed = fmean(values[:period])
    for index, value in enumerate(values):
        if index < period - 1:
            results.append(None)
        elif index == period - 1:
            results.append(seed)
        else:
            previous = results[-1]
            assert previous is not None
            results.append((value - previous) * multiplier + previous)
    return results


def _latest_ema(values: list[float], period: int) -> float | None:
    series = _ema_series(values, period)
    return series[-1] if series else None


def _rsi(values: list[float], period: int) -> float | None:
    if len(values) < period + 1:
        return None
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    gains = [max(delta, 0.0) for delta in deltas]
    losses = [abs(min(delta, 0.0)) for delta in deltas]
    avg_gain = fmean(gains[-period:])
    avg_loss = fmean(losses[-period:])
    if avg_loss == 0:
        return 100.0
    relative_strength = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + relative_strength))


def _macd(values: list[float]) -> tuple[float | None, float | None, float | None]:
    ema12_series = _ema_series(values, 12)
    ema26_series = _ema_series(values, 26)
    macd_series: list[float] = []
    for ema12, ema26 in zip(ema12_series, ema26_series):
        if ema12 is None or ema26 is None:
            continue
        macd_series.append(ema12 - ema26)
    if not macd_series:
        return None, None, None
    signal_series = _ema_series(macd_series, 9)
    macd_line = macd_series[-1]
    macd_signal = signal_series[-1] if signal_series else None
    macd_hist = macd_line - macd_signal if macd_signal is not None else None
    return macd_line, macd_signal, macd_hist


def _bollinger(values: list[float], period: int = 20, num_std: float = 2.0) -> tuple[float | None, float | None, float | None]:
    if len(values) < period:
        return None, None, None
    window = values[-period:]
    middle = fmean(window)
    sigma = stddev_or_none(window)
    if sigma is None:
        return middle, middle, middle
    return middle, middle + num_std * sigma, middle - num_std * sigma


def _atr(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    if len(bars) < period + 1:
        return None
    true_ranges: list[float] = []
    previous_close: float | None = None
    for bar in bars:
        high = to_float(bar.get("high"))
        low = to_float(bar.get("low"))
        close = to_float(bar.get("close"))
        if high is None or low is None or close is None:
            previous_close = close
            continue
        if previous_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(true_range)
        previous_close = close
    if len(true_ranges) < period:
        return None
    return fmean(true_ranges[-period:])


def _volatility_20d(closes: list[float]) -> float | None:
    if len(closes) < 21:
        return None
    returns = [
        simple_return(closes[index], closes[index - 1])
        for index in range(1, len(closes))
    ]
    return stddev_or_none(returns[-20:])


class Phase3FeatureStoreProcess:
    def __init__(
        self,
        output_root: Path | None = None,
        daily_history_root: Path | None = None,
        include_today_after_local_hour: int = 18,
        symbols: set[str] | None = None,
    ) -> None:
        self.output_root = Path(output_root) if output_root else default_output_root()
        self.daily_history_root = Path(daily_history_root) if daily_history_root else default_daily_history_root()
        self.include_today_after_local_hour = max(0, min(int(include_today_after_local_hour), 23))
        self.symbols = {symbol.upper() for symbol in symbols} if symbols else None
        self._input_cache: dict[str, CachedBundleFile] = {}
        self._first_scan = True

    def run_once(self) -> dict[str, int]:
        dirty_items, self._first_scan = refresh_single_input_cache(
            self.output_root,
            PHASE2_FOLDER,
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

    def _load_history(self, symbol: str) -> tuple[list[dict[str, Any]], list[str]]:
        history_root = self.daily_history_root / symbol / "historical_price"
        records: dict[str, dict[str, Any]] = {}
        source_paths: list[str] = []
        if not history_root.exists():
            return [], source_paths

        for path in sorted(history_root.rglob("*.json")):
            source_paths.append(str(path))
            for record in load_json_records(path):
                normalized = _normalized_daily_bar(record)
                if normalized is None:
                    continue
                records[normalized["date"]] = normalized

        ordered = [records[key] for key in sorted(records)]
        return ordered, source_paths

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        source_bundle = find_cached_bundle(self._input_cache, symbol, trading_date)
        if source_bundle is None:
            return None

        payload = source_bundle.payload
        market_state_frames = payload.get("market_state_frames") or []
        base_feature_frames = payload.get("base_feature_frames") or []
        if len(market_state_frames) != len(base_feature_frames):
            raise ValueError(f"phase2 frame mismatch for {symbol} {trading_date}")

        daily_history, history_paths = self._load_history(symbol)
        local_now = datetime.now().astimezone()
        include_today_bar = (
            local_now.strftime("%Y-%m-%d") > trading_date
            or (
                local_now.strftime("%Y-%m-%d") == trading_date
                and local_now.timetz().replace(tzinfo=None) >= time_value(self.include_today_after_local_hour, 0)
            )
        )
        if include_today_bar:
            lookback_bars = [bar for bar in daily_history if bar.get("date") <= trading_date]
        else:
            lookback_bars = [bar for bar in daily_history if bar.get("date") < trading_date]
        closes = [to_float(bar.get("close")) for bar in lookback_bars if to_float(bar.get("close")) is not None]
        closes = [value for value in closes if value is not None]
        volumes = [to_float(bar.get("volume")) for bar in lookback_bars if to_float(bar.get("volume")) is not None]
        volumes = [value for value in volumes if value is not None]

        ma_5 = _sma(closes, 5)
        ma_10 = _sma(closes, 10)
        ma_20 = _sma(closes, 20)
        ma_50 = _sma(closes, 50)
        ma_100 = _sma(closes, 100)
        ma_200 = _sma(closes, 200)
        ema_12 = _latest_ema(closes, 12)
        ema_20 = _latest_ema(closes, 20)
        ema_26 = _latest_ema(closes, 26)
        rsi_14_d = _rsi(closes, 14)
        macd_line_d, macd_signal_d, macd_hist_d = _macd(closes)
        bollinger_mid_20, bollinger_upper_20, bollinger_lower_20 = _bollinger(closes, 20, 2.0)
        atr_14 = _atr(lookback_bars, 14)
        volatility_20d = _volatility_20d(closes)
        avg_volume_20d = fmean(volumes[-20:]) if len(volumes) >= 20 else None
        ma20_series = _rolling_sma(closes, 20)
        ma20_current = ma20_series[-1] if ma20_series else None
        ma20_5d_ago = ma20_series[-6] if len(ma20_series) >= 6 else None
        ma20_slope_5d = safe_div((ma20_current - ma20_5d_ago) if ma20_current is not None and ma20_5d_ago is not None else None, ma20_5d_ago)
        ma20_ma200_ratio = safe_div(ma_20, ma_200)

        previous_bar = None
        for bar in reversed(daily_history):
            if bar.get("date") < trading_date:
                previous_bar = bar
                break
        previous_close = to_float(previous_bar.get("close")) if isinstance(previous_bar, dict) else None
        daily_context = None
        if market_state_frames:
            source_input_path = source_bundle.payload.get("source_layer3_input_bundle_path")
            if source_input_path:
                phase0_bundle = load_json_records(Path(source_input_path))
                if phase0_bundle and isinstance(phase0_bundle[0], dict):
                    daily_branch = phase0_bundle[0].get("daily_branch") or {}
                    daily_context = daily_branch.get("daily_context") or daily_branch.get("normalized_daily_input")

        if daily_context is None:
            daily_context = {}

        daily_total_volume = to_float(daily_context.get("total_volume"))
        buy_foreign_quantity = to_float(daily_context.get("buy_foreign_quantity"))
        sell_foreign_quantity = to_float(daily_context.get("sell_foreign_quantity"))
        final_foreign_net_volume = None
        if buy_foreign_quantity is not None and sell_foreign_quantity is not None:
            final_foreign_net_volume = buy_foreign_quantity - sell_foreign_quantity

        feature_store_frames: list[dict[str, Any]] = []
        for market_frame, base_frame in zip(market_state_frames, base_feature_frames):
            current_price = to_float(market_frame.get("current_price"))
            day_open = to_float(market_frame.get("day_open"))
            cum_volume = to_float(market_frame.get("cum_volume"))
            vwap_intraday = to_float(market_frame.get("vwap_intraday"))
            intraday_pos_in_range = to_float(market_frame.get("intraday_pos_in_range"))
            day_high_so_far = to_float(market_frame.get("day_high_so_far"))
            day_low_so_far = to_float(market_frame.get("day_low_so_far"))
            reference_time = parse_iso_datetime(market_frame.get("reference_time_local"))

            gap_pct = None
            if day_open not in (None, 0.0) and previous_close not in (None, 0.0):
                gap_pct = day_open / previous_close - 1.0

            relative_volume_day = safe_div(cum_volume, avg_volume_20d)
            distance_to_day_high = None
            if current_price is not None and day_high_so_far not in (None, 0.0):
                distance_to_day_high = current_price / day_high_so_far - 1.0
            distance_to_day_low = None
            if current_price is not None and day_low_so_far not in (None, 0.0):
                distance_to_day_low = current_price / day_low_so_far - 1.0

            foreign_net_volume_day = None
            foreign_net_ratio_day = None
            if reference_time is not None and reference_time.timetz().replace(tzinfo=None) >= time_value(14, 45):
                foreign_net_volume_day = final_foreign_net_volume
                foreign_net_ratio_day = safe_div(final_foreign_net_volume, daily_total_volume)

            signed_volume_10s = to_float(base_frame.get("signed_volume_10s"))
            book_imbalance_l1 = to_float(base_frame.get("book_imbalance_l1"))
            signed_volume_x_imbalance = None
            if signed_volume_10s is not None and book_imbalance_l1 is not None:
                signed_volume_x_imbalance = signed_volume_10s * book_imbalance_l1

            price_to_vwap_x_relative_volume = None
            price_to_vwap = safe_div(current_price, vwap_intraday)
            if price_to_vwap is not None and relative_volume_day is not None:
                price_to_vwap_x_relative_volume = price_to_vwap * relative_volume_day

            feature_store_frames.append(
                {
                    "frame_no": market_frame.get("frame_no"),
                    "reference_time_local": market_frame.get("reference_time_local"),
                    "reference_time_utc": market_frame.get("reference_time_utc"),
                    "ma_5": ma_5,
                    "ma_10": ma_10,
                    "ma_20": ma_20,
                    "ma_50": ma_50,
                    "ma_100": ma_100,
                    "ma_200": ma_200,
                    "ema_12": ema_12,
                    "ema_20": ema_20,
                    "ema_26": ema_26,
                    "rsi_14_d": rsi_14_d,
                    "macd_line_d": macd_line_d,
                    "macd_signal_d": macd_signal_d,
                    "macd_hist_d": macd_hist_d,
                    "bollinger_mid_20": bollinger_mid_20,
                    "bollinger_upper_20": bollinger_upper_20,
                    "bollinger_lower_20": bollinger_lower_20,
                    "atr_14": atr_14,
                    "volatility_20d": volatility_20d,
                    "avg_volume_20d": avg_volume_20d,
                    "price_to_ma20": safe_div(current_price, ma_20),
                    "price_to_ma200": safe_div(current_price, ma_200),
                    "ma20_slope_5d": ma20_slope_5d,
                    "ma20_ma200_ratio": ma20_ma200_ratio,
                    "gap_pct": gap_pct,
                    "relative_volume_day": relative_volume_day,
                    "foreign_net_volume_day": foreign_net_volume_day,
                    "foreign_net_ratio_day": foreign_net_ratio_day,
                    "intraday_pos_in_range": intraday_pos_in_range,
                    "distance_to_day_high": distance_to_day_high,
                    "distance_to_day_low": distance_to_day_low,
                    "signed_volume_x_imbalance": signed_volume_x_imbalance,
                    "price_to_vwap_x_relative_volume": price_to_vwap_x_relative_volume,
                }
            )

        first_frame = feature_store_frames[0] if feature_store_frames else {}
        last_frame = feature_store_frames[-1] if feature_store_frames else {}
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
                "daily_technical_inputs": "daily technical indicators are computed from data_ingestion historical_price before trading_date, except the current day is included once local machine time passes the configured cutoff hour",
                "foreign_net_fields": "only populated after 14:45 local to avoid intraday future leakage",
                "price_to_ma": "current_price / moving_average",
                "relative_volume_day": "cum_volume / avg_volume_20d",
                "distance_to_day_high": "current_price / day_high_so_far - 1",
                "distance_to_day_low": "current_price / day_low_so_far - 1",
            },
            "daily_history_metadata": {
                "source_root": str(self.daily_history_root / symbol / "historical_price"),
                "source_file_count": len(history_paths),
                "available_history_days": len(daily_history),
                "available_pre_session_days": len(lookback_bars),
                "include_today_bar": include_today_bar,
                "include_today_after_local_hour": self.include_today_after_local_hour,
                "local_time_used_for_cutoff": local_now.isoformat(),
                "latest_history_date": daily_history[-1]["date"] if daily_history else None,
            },
            "frame_count": len(feature_store_frames),
            "reference_time_summary": {
                "first_reference_time_local": first_frame.get("reference_time_local"),
                "last_reference_time_local": last_frame.get("reference_time_local"),
            },
            "market_state_frames": market_state_frames,
            "base_feature_frames": base_feature_frames,
            "feature_store_frames": feature_store_frames,
        }


__all__ = [
    "BUNDLE_FOLDER",
    "BUNDLE_TYPE",
    "PHASE_NAME",
    "Phase3FeatureStoreProcess",
]

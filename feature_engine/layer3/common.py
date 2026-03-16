from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, time as time_value, timezone
from pathlib import Path
from statistics import fmean
from typing import Any, Iterable


@dataclass(frozen=True)
class FileSignature:
    size: int
    mtime_ns: int


@dataclass
class CachedBundleFile:
    symbol: str
    trading_date: str
    path: Path
    signature: FileSignature
    payload: dict[str, Any]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_input_root() -> Path:
    return repo_root() / "data_processing" / "data"


def default_output_root() -> Path:
    return repo_root() / "feature_engine" / "data"


def default_daily_history_root() -> Path:
    return repo_root() / "data_ingestion" / "data"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_symbol(symbol: str) -> str:
    if not symbol:
        return "UNKNOWN"
    cleaned = str(symbol).strip().replace("/", "_").replace("\\", "_")
    return cleaned or "UNKNOWN"


def is_valid_date_text(value: str | None) -> bool:
    if not value or len(value) < 10:
        return False
    candidate = value[:10]
    try:
        datetime.strptime(candidate, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def coerce_date_text(value: Any) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    if not candidate:
        return None
    if is_valid_date_text(candidate):
        return candidate[:10]
    return None


def normalize_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def load_json_records(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8-sig")
    if not text.strip():
        return []

    try:
        payload = json.loads(text)
        return normalize_payload(payload)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    cursor = 0
    text_len = len(text)
    records: list[dict[str, Any]] = []

    while cursor < text_len:
        while cursor < text_len and text[cursor].isspace():
            cursor += 1
        if cursor >= text_len:
            break
        payload, cursor = decoder.raw_decode(text, cursor)
        records.extend(normalize_payload(payload))

    return records


def load_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


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


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def safe_div(numerator: Any, denominator: Any) -> float | None:
    if numerator is None or denominator in (None, 0, 0.0):
        return None
    return float(numerator) / float(denominator)


def mean_or_none(values: Iterable[float | None]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return None
    return fmean(filtered)


def stddev_or_none(values: Iterable[float | None]) -> float | None:
    filtered = [float(value) for value in values if value is not None]
    if len(filtered) < 2:
        return None
    mean_value = fmean(filtered)
    variance = sum((value - mean_value) ** 2 for value in filtered) / (len(filtered) - 1)
    return math.sqrt(variance)


def price_token(value: Any) -> float | None:
    numeric = to_float(value)
    if numeric is None:
        return None
    return round(numeric, 6)


def simple_return(current: Any, previous: Any) -> float | None:
    if current is None or previous in (None, 0, 0.0):
        return None
    return float(current) / float(previous) - 1.0


def signed_trade_volume(side: Any, volume: Any) -> float | None:
    numeric_volume = to_float(volume)
    if numeric_volume is None:
        return None
    side_text = str(side).upper() if side is not None else ""
    if side_text == "B":
        return numeric_volume
    if side_text == "S":
        return -numeric_volume
    return 0.0


def sort_prices_for_side(side: str, prices: Iterable[float]) -> list[float]:
    numeric_prices = [float(price) for price in prices]
    reverse = side.lower() == "bid"
    return sorted(numeric_prices, reverse=reverse)


def is_session_time(moment: datetime | None) -> bool:
    if moment is None:
        return False
    local_time = moment.timetz().replace(tzinfo=None)
    return (
        time_value(9, 0) <= local_time <= time_value(11, 30)
        or time_value(13, 0) <= local_time <= time_value(14, 45)
    )


def is_auction_time(moment: datetime | None) -> bool:
    if moment is None:
        return False
    local_time = moment.timetz().replace(tzinfo=None)
    return (
        time_value(9, 0) <= local_time < time_value(9, 5)
        or time_value(14, 40) <= local_time <= time_value(14, 45)
    )


def is_near_close_time(moment: datetime | None) -> bool:
    if moment is None:
        return False
    local_time = moment.timetz().replace(tzinfo=None)
    return time_value(14, 30) <= local_time <= time_value(14, 45)


def output_bundle_path(
    output_root: Path,
    symbol: str,
    folder_name: str,
    trading_date: str,
) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / folder_name / trading_date[:4] / f"{trading_date}.json"


def deterministic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def refresh_single_input_cache(
    root: Path,
    input_folder: str,
    symbols: set[str] | None,
    cache: dict[str, CachedBundleFile],
    first_scan: bool,
) -> tuple[set[tuple[str, str]], bool]:
    dirty_items: set[tuple[str, str]] = set()
    discovered_paths: set[str] = set()

    if not root.exists():
        return dirty_items, first_scan

    for symbol_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        symbol = symbol_dir.name.upper()
        if symbols and symbol not in symbols:
            continue

        input_dir = symbol_dir / input_folder
        if not input_dir.exists():
            continue

        for path in sorted(input_dir.rglob("*.json")):
            cache_key = str(path.resolve())
            discovered_paths.add(cache_key)

            stat = path.stat()
            signature = FileSignature(size=stat.st_size, mtime_ns=stat.st_mtime_ns)
            cached = cache.get(cache_key)
            if cached and cached.signature == signature:
                continue

            previous_date = cached.trading_date if cached else None
            payload = load_json_object(path)
            symbol_value = sanitize_symbol(str(payload.get("symbol") or symbol).upper())
            trading_date = coerce_date_text(payload.get("trading_date")) or coerce_date_text(path.stem)
            if not trading_date:
                raise ValueError(f"missing trading_date in {path}")

            cache[cache_key] = CachedBundleFile(
                symbol=symbol_value,
                trading_date=trading_date,
                path=path,
                signature=signature,
                payload=payload,
            )

            dirty_items.add((symbol_value, trading_date))
            if previous_date and previous_date != trading_date:
                dirty_items.add((symbol_value, previous_date))

    removed_paths = set(cache) - discovered_paths
    for cache_key in removed_paths:
        cached = cache.pop(cache_key)
        dirty_items.add((cached.symbol, cached.trading_date))

    if first_scan:
        for cached in cache.values():
            dirty_items.add((cached.symbol, cached.trading_date))
        first_scan = False

    return dirty_items, first_scan


def find_cached_bundle(
    cache: dict[str, CachedBundleFile],
    symbol: str,
    trading_date: str,
) -> CachedBundleFile | None:
    matches = [
        cached
        for cached in cache.values()
        if cached.symbol == symbol and cached.trading_date == trading_date
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda item: item.path.as_posix())[-1]

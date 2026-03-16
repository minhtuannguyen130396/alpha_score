from __future__ import annotations

import json
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any


SOURCE_GROUPS = (
    "historical_price",
    "updatelastprices",
    "updateorderbooks",
    "updatetrades",
)

BUNDLE_FOLDER = "phase0_raw_source_intake"
LOG_FOLDER = "phase0_raw_source_intake_logs"
LOCAL_TRADING_TZ = timezone(timedelta(hours=7))
SESSION_START_LOCAL = dt_time(hour=9, minute=0)
LUNCH_BREAK_START_LOCAL = dt_time(hour=11, minute=30)
LUNCH_BREAK_END_LOCAL = dt_time(hour=13, minute=0)
SESSION_END_LOCAL = dt_time(hour=14, minute=45)


@dataclass(frozen=True)
class FileSignature:
    size: int
    mtime_ns: int


@dataclass
class CachedSourceFile:
    symbol: str
    source_group: str
    path: Path
    signature: FileSignature
    records_by_date: dict[str, list[dict[str, Any]]]

    @property
    def dates(self) -> set[str]:
        return set(self.records_by_date)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_input_root() -> Path:
    return repo_root() / "data_ingestion" / "data"


def default_output_root() -> Path:
    return repo_root() / "data_processing" / "data"


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
    records: list[dict[str, Any]] = []
    text_len = len(text)

    while cursor < text_len:
        while cursor < text_len and text[cursor].isspace():
            cursor += 1

        if cursor >= text_len:
            break

        payload, cursor = decoder.raw_decode(text, cursor)
        records.extend(normalize_payload(payload))

    return records


def parse_event_time_utc(value: Any) -> datetime | None:
    if value is None:
        return None

    try:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value).strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_local_event_time(value: Any) -> datetime | None:
    parsed_utc = parse_event_time_utc(value)
    if parsed_utc is None:
        return None
    return parsed_utc.astimezone(LOCAL_TRADING_TZ)


def is_record_in_local_session(event_time_value: Any) -> bool:
    local_event_time = to_local_event_time(event_time_value)
    if local_event_time is None:
        return False

    current_time = local_event_time.time().replace(tzinfo=None)
    in_morning_session = SESSION_START_LOCAL <= current_time < LUNCH_BREAK_START_LOCAL
    in_afternoon_session = LUNCH_BREAK_END_LOCAL <= current_time <= SESSION_END_LOCAL
    return in_morning_session or in_afternoon_session


def output_bundle_path(output_root: Path, symbol: str, trading_date: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / BUNDLE_FOLDER / trading_date[:4] / f"{trading_date}.json"


def output_log_path(output_root: Path, symbol: str, trading_date: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    return output_root / safe_symbol / LOG_FOLDER / trading_date[:4] / f"{trading_date}.jsonl"


class Phase0RawSourceIntakeProcess:
    def __init__(
        self,
        input_root: Path | None = None,
        output_root: Path | None = None,
        symbols: set[str] | None = None,
        include_historical_only: bool = False,
    ) -> None:
        self.input_root = Path(input_root) if input_root else default_input_root()
        self.output_root = Path(output_root) if output_root else default_output_root()
        self.symbols = {symbol.upper() for symbol in symbols} if symbols else None
        self.include_historical_only = include_historical_only
        self._source_cache: dict[str, CachedSourceFile] = {}
        self._first_scan = True

    def run_forever(self, interval_seconds: float = 5.0) -> None:
        interval_seconds = max(float(interval_seconds), 0.1)
        self._stdout(
            f"starting phase0 raw source intake: input={self.input_root} output={self.output_root} interval={interval_seconds}s"
        )
        while True:
            self.run_once()
            time.sleep(interval_seconds)

    def run_once(self) -> dict[str, int]:
        dirty_items = self._refresh_source_cache()
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
                self._stdout(f"failed bundle for {symbol} {trading_date}: {exc}")

        if not dirty_items:
            self._stdout("phase0 cycle complete: idle")
        else:
            self._stdout(
                f"phase0 cycle complete: dirty={len(dirty_items)} built={built_count} removed={removed_count} failed={failed_count}"
            )

        return {
            "dirty": len(dirty_items),
            "built": built_count,
            "removed": removed_count,
            "failed": failed_count,
        }

    def _stdout(self, message: str) -> None:
        print(f"[{utc_now_iso()}] {message}")

    def _refresh_source_cache(self) -> set[tuple[str, str]]:
        dirty_items: set[tuple[str, str]] = set()
        discovered_paths: set[str] = set()

        if not self.input_root.exists():
            self._stdout(f"input root not found: {self.input_root}")
            return dirty_items

        for symbol_dir in sorted(path for path in self.input_root.iterdir() if path.is_dir()):
            symbol = symbol_dir.name.upper()
            if self.symbols and symbol not in self.symbols:
                continue

            for source_group in SOURCE_GROUPS:
                source_dir = symbol_dir / source_group
                if not source_dir.exists():
                    continue

                for path in sorted(source_dir.rglob("*.json")):
                    cache_key = str(path.resolve())
                    discovered_paths.add(cache_key)

                    stat = path.stat()
                    signature = FileSignature(size=stat.st_size, mtime_ns=stat.st_mtime_ns)
                    cached = self._source_cache.get(cache_key)
                    if cached and cached.signature == signature:
                        continue

                    previous_dates = cached.dates if cached else set()
                    try:
                        records_by_date = self._load_records_by_date(path, source_group)
                    except Exception as exc:
                        self._stdout(f"failed to parse {path}: {exc}")
                        continue

                    self._source_cache[cache_key] = CachedSourceFile(
                        symbol=symbol,
                        source_group=source_group,
                        path=path,
                        signature=signature,
                        records_by_date=records_by_date,
                    )

                    for trading_date in previous_dates | set(records_by_date):
                        dirty_items.add((symbol, trading_date))

        removed_paths = set(self._source_cache) - discovered_paths
        for cache_key in removed_paths:
            cached = self._source_cache.pop(cache_key)
            for trading_date in cached.dates:
                dirty_items.add((cached.symbol, trading_date))

        if self._first_scan:
            for cached in self._source_cache.values():
                for trading_date in cached.dates:
                    dirty_items.add((cached.symbol, trading_date))
            self._first_scan = False

        return dirty_items

    def _load_records_by_date(
        self,
        path: Path,
        source_group: str,
    ) -> dict[str, list[dict[str, Any]]]:
        fallback_date = coerce_date_text(path.stem)
        records_by_date: dict[str, list[dict[str, Any]]] = {}

        for record in load_json_records(path):
            if not self._should_keep_record(record, source_group):
                continue
            trading_date = self._extract_record_date(record, source_group, fallback_date)
            if not trading_date:
                continue
            records_by_date.setdefault(trading_date, []).append(record)

        return records_by_date

    def _extract_record_date(
        self,
        record: dict[str, Any],
        source_group: str,
        fallback_date: str | None,
    ) -> str | None:
        candidates: list[Any]
        if source_group == "historical_price":
            candidates = [record.get("date"), record.get("trading_date"), fallback_date]
        elif source_group == "updatetrades":
            local_event_time = to_local_event_time(record.get("event_time"))
            candidates = [
                local_event_time.isoformat() if local_event_time else None,
                record.get("trading_date"),
                record.get("event_time"),
                record.get("ts"),
                fallback_date,
            ]
        else:
            local_event_time = to_local_event_time(record.get("event_time"))
            candidates = [
                local_event_time.isoformat() if local_event_time else None,
                record.get("event_time"),
                record.get("ts"),
                record.get("date"),
                fallback_date,
            ]

        for candidate in candidates:
            trading_date = coerce_date_text(candidate)
            if trading_date:
                return trading_date
        return None

    def _should_keep_record(self, record: dict[str, Any], source_group: str) -> bool:
        if source_group == "historical_price":
            return True
        return is_record_in_local_session(record.get("event_time"))

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
                self._stdout(f"removed stale bundle: {symbol} {trading_date}")
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
                "record_counts": bundle["record_counts"],
                "source_group_status": bundle["source_group_status"],
                "source_file_count": bundle["source_file_count"],
                "latest_source_mtime_utc": bundle["latest_source_mtime_utc"],
            },
        )
        self._stdout(
            f"built bundle: {symbol} {trading_date} counts={bundle['record_counts']}"
        )
        return "built"

    def _build_bundle(self, symbol: str, trading_date: str) -> dict[str, Any] | None:
        grouped_records = {
            "historical_price": [],
            "updatelastprices": [],
            "updateorderbooks": [],
            "updatetrades": [],
        }
        source_files: dict[str, list[str]] = {source_group: [] for source_group in SOURCE_GROUPS}
        latest_source_mtime_ns: int | None = None

        for cached in sorted(self._source_cache.values(), key=lambda item: (item.symbol, item.path.as_posix())):
            if cached.symbol != symbol:
                continue

            records = cached.records_by_date.get(trading_date)
            if not records:
                continue

            grouped_records[cached.source_group].extend(records)
            source_files[cached.source_group].append(str(cached.path))

            if latest_source_mtime_ns is None or cached.signature.mtime_ns > latest_source_mtime_ns:
                latest_source_mtime_ns = cached.signature.mtime_ns

        if not any(grouped_records.values()):
            return None

        has_intraday_records = any(
            grouped_records[source_group]
            for source_group in SOURCE_GROUPS
            if source_group != "historical_price"
        )
        if not has_intraday_records and not self.include_historical_only:
            return None

        latest_source_mtime_utc = None
        if latest_source_mtime_ns is not None:
            latest_source_mtime_utc = datetime.fromtimestamp(
                latest_source_mtime_ns / 1_000_000_000,
                tz=timezone.utc,
            ).isoformat()

        record_counts = {
            "raw_daily_records": len(grouped_records["historical_price"]),
            "raw_lastprice_records": len(grouped_records["updatelastprices"]),
            "raw_orderbook_records": len(grouped_records["updateorderbooks"]),
            "raw_trade_records": len(grouped_records["updatetrades"]),
        }

        source_group_status = {
            "historical_price": record_counts["raw_daily_records"] > 0,
            "updatelastprices": record_counts["raw_lastprice_records"] > 0,
            "updateorderbooks": record_counts["raw_orderbook_records"] > 0,
            "updatetrades": record_counts["raw_trade_records"] > 0,
        }

        return {
            "bundle_type": "raw_source_bundle",
            "phase": "phase0_raw_source_intake",
            "symbol": symbol,
            "trading_date": trading_date,
            "generated_at_utc": utc_now_iso(),
            "session_filter_local": {
                "timezone": "UTC+07:00",
                "allowed_windows": ["09:00-11:29:59.999999", "13:00-14:45:00"]
            },
            "is_complete": all(source_group_status.values()),
            "has_intraday_records": has_intraday_records,
            "source_group_status": source_group_status,
            "source_file_count": sum(len(paths) for paths in source_files.values()),
            "latest_source_mtime_utc": latest_source_mtime_utc,
            "source_files": source_files,
            "record_counts": record_counts,
            "raw_daily_records": grouped_records["historical_price"],
            "raw_lastprice_records": grouped_records["updatelastprices"],
            "raw_orderbook_records": grouped_records["updateorderbooks"],
            "raw_trade_records": grouped_records["updatetrades"],
        }

    def _log_event(self, symbol: str, trading_date: str, payload: dict[str, Any]) -> None:
        path = output_log_path(self.output_root, symbol, trading_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

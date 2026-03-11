from pathlib import Path

from settings import DATA_ROOT


def sanitize_symbol(symbol: str) -> str:
    if not symbol:
        return "UNKNOWN"
    cleaned = str(symbol).strip().replace("/", "_").replace("\\", "_")
    return cleaned or "UNKNOWN"


def build_data_path(symbol: str, data_type: str, year: str, date_str: str) -> Path:
    safe_symbol = sanitize_symbol(symbol)
    safe_type = str(data_type).strip().lower()
    return DATA_ROOT / safe_symbol / safe_type / year / f"{date_str}.json"

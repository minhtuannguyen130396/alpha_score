import json
from pathlib import Path

import msgpack

from storage.paths import build_data_path, sanitize_symbol
from utils.time_utils import now_parts


def append_json_line(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")



def save_parsed(command_name: str, symbol: str, records) -> None:
    _, year, date_str, iso_ts = now_parts()
    path = build_data_path(symbol, command_name.lower(), year, date_str)
    for record in records:
        append_json_line(
            path,
            {
                "ts": iso_ts,
                "command": command_name,
                "symbol": sanitize_symbol(symbol),
                **record,
            },
        )

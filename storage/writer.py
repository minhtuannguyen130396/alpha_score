import json
from pathlib import Path

import msgpack

from storage.paths import build_data_path, sanitize_symbol
from utils.time_utils import now_parts


def append_json_line(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_raw(symbol: str, raw_bytes: bytes) -> None:
    _, year, date_str, iso_ts = now_parts()
    path = build_data_path(symbol, "raw", year, date_str)
    append_json_line(
        path,
        {
            "ts": iso_ts,
            "symbol": sanitize_symbol(symbol),
            "raw_hex": raw_bytes.hex(),
            "raw_len": len(raw_bytes),
        },
    )


def save_raw_data_decode(symbol: str, raw_bytes: bytes) -> None:
    _, year, date_str, iso_ts = now_parts()
    path = build_data_path(symbol, "raw_data_decode", year, date_str)

    utf8_text = None
    try:
        utf8_text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        utf8_text = None

    ascii_preview = "".join(chr(b) if 32 <= b <= 126 else "." for b in raw_bytes)

    msgpack_decoded = None
    try:
        unpacker = msgpack.Unpacker(raw=False)
        unpacker.feed(raw_bytes)
        msgpack_decoded = list(unpacker)
    except Exception:
        msgpack_decoded = None

    append_json_line(
        path,
        {
            "ts": iso_ts,
            "symbol": sanitize_symbol(symbol),
            "raw_len": len(raw_bytes),
            "utf8_text": utf8_text,
            "ascii_preview": ascii_preview,
            "msgpack_decoded": msgpack_decoded,
        },
    )


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

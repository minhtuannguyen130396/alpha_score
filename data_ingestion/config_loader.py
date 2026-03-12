import base64
import json

import msgpack

from settings import COMMAND_REFER_PATH, SYMBOLS_PATH


def build_subscribe_base64(symbol: str, chunk_index: str = "0") -> str:
    """Build a base64 SubscribeQuotes command for a single symbol."""
    message = [1, {}, chunk_index, "SubscribeQuotes", [symbol]]
    packed_message = msgpack.packb(message, use_bin_type=True)
    packed_length = msgpack.packb(len(packed_message), use_bin_type=True)
    payload = packed_length + packed_message
    return base64.b64encode(payload).decode("ascii")


def _load_legacy_command_map():
    with COMMAND_REFER_PATH.open("r", encoding="utf-8") as handle:
        rows = json.load(handle)

    command_map = {}
    for row in rows:
        symbol = str(row["symbol"]).strip().upper()
        command = str(row["command"]).strip()
        command_map[symbol] = command
    return command_map


def load_command_map():
    legacy_command_map = _load_legacy_command_map()

    command_map = {}
    missing_symbols = []

    for symbol in load_symbols():
        try:
            command_map[symbol] = build_subscribe_base64(symbol)
        except Exception:
            legacy_command = legacy_command_map.get(symbol)
            if legacy_command:
                command_map[symbol] = legacy_command
                continue
            missing_symbols.append(symbol)

    if missing_symbols:
        raise ValueError(
            f"Missing command mapping for symbols: {', '.join(missing_symbols)}"
        )

    return command_map


def load_symbols():
    with SYMBOLS_PATH.open("r", encoding="utf-8") as handle:
        rows = json.load(handle)
    return [str(symbol).strip().upper() for symbol in rows]


def validate_symbols(symbols, command_map):
    missing = [symbol for symbol in symbols if symbol not in command_map]
    if missing:
        raise ValueError(f"Missing command mapping for symbols: {', '.join(missing)}")

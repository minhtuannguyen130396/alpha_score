import json

from settings import COMMAND_REFER_PATH, SYMBOLS_PATH


def load_command_map():
    with COMMAND_REFER_PATH.open("r", encoding="utf-8") as handle:
        rows = json.load(handle)

    command_map = {}
    for row in rows:
        symbol = str(row["symbol"]).strip().upper()
        command = str(row["command"]).strip()
        command_map[symbol] = command
    return command_map


def load_symbols():
    with SYMBOLS_PATH.open("r", encoding="utf-8") as handle:
        rows = json.load(handle)
    return [str(symbol).strip().upper() for symbol in rows]


def validate_symbols(symbols, command_map):
    missing = [symbol for symbol in symbols if symbol not in command_map]
    if missing:
        raise ValueError(f"Missing command mapping for symbols: {', '.join(missing)}")

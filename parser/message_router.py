from parser.update_parser import MarketDataDecoder
from storage.writer import save_parsed, save_raw
from utils.logging_utils import log_data

DECODER = MarketDataDecoder()


def _filter_records_for_symbol(symbol: str, command_name: str, records):
    filtered_records = []
    normalized_symbol = str(symbol).strip().upper()

    for record in records:
        record_symbol = record.get("symbol")
        if record_symbol is None:
            raise ValueError(f"{command_name} record is missing symbol")

        if str(record_symbol).strip().upper() == normalized_symbol:
            filtered_records.append(record)

    return filtered_records


def route_binary_message(symbol: str, msg_bytes: bytes) -> None:
    save_raw(symbol, msg_bytes)
    try:
        # One raw frame can contain many supported patterns, so decode first
        # and then persist each normalized pattern independently.
        decoded_patterns = DECODER.decode_bytes(msg_bytes, symbol)
    except Exception as exc:
        log_data("Decode error", str(exc), symbol)
        return

    if not decoded_patterns:
        log_data("Command", "UNKNOWN (no Update* marker found)", symbol)
        return

    for pattern in decoded_patterns:
        cmd_name = pattern["command"]
        filtered_records = _filter_records_for_symbol(symbol, cmd_name, pattern["records"])
        if not filtered_records:
            continue

        log_data("Command detected", cmd_name, symbol)
        save_parsed(cmd_name, symbol, filtered_records)

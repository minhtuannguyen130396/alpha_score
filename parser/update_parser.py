from datetime import datetime, timezone

import msgpack

from utils.logging_utils import log_data


class MarketDataDecoder:
    """
    Central decoder for websocket market-data frames.

    Current responsibilities:
    - unpack a full MessagePack stream from raw bytes/hex
    - extract every Update* invocation inside the same frame
    - normalize supported patterns into JSON-friendly dict records

    Planned extensions:
    - add dedicated tick-data decoding when a stable tick pattern is confirmed
    - add more Update* handlers without changing router/storage flow
    """

    def __init__(self):
        self._decoders = {
            "UpdateLastPrices": self._decode_update_last_prices,
            "UpdateRefPrices": self._decode_update_ref_prices,
            "UpdateOrderBooks": self._decode_update_order_books,
            "UpdateForeignerStats": self._decode_update_foreigner_stats,
            "UpdateSymbols": self._decode_update_symbols,
            # TODO: Register tick-data decoder here once the tick payload
            # structure is confirmed from live raw samples.
        }
        self._command_markers = {
            command_name.encode("ascii"): command_name
            for command_name in self._decoders
        }

    def decode_hex(self, raw_hex: str, symbol: str):
        return self.decode_bytes(bytes.fromhex(raw_hex), symbol)

    def decode_bytes(self, raw_bytes: bytes, symbol: str):
        # Decode the entire stream because a single websocket frame may contain
        # multiple Update* invocations concatenated together.
        try:
            unpacker = msgpack.Unpacker(raw=True, strict_map_key=False)
            unpacker.feed(raw_bytes)

            decoded_patterns = []
            for item in unpacker:
                decoded_patterns.extend(self._decode_item(item, symbol))
            return decoded_patterns
        except Exception as exc:
            log_data("Decode fallback", str(exc), symbol)
            return self._decode_by_markers(raw_bytes, symbol)

    def _decode_item(self, item, symbol: str):
        invocations = []
        self._collect_update_invocations(item, invocations)

        decoded_patterns = []
        for invocation in invocations:
            command_name = invocation[3].decode("ascii", "ignore")
            payload = invocation[4] if len(invocation) > 4 else []
            records = payload[0] if payload and isinstance(payload, list) and payload[0] else []

            decoder = self._decoders.get(command_name)
            if not decoder:
                # Unsupported patterns are skipped here so the router can keep
                # processing the rest of the frame without failing hard.
                continue

            normalized_records = []
            for record in records:
                try:
                    normalized_records.append(decoder(record))
                except Exception as exc:
                    log_data("Decode record error", f"{command_name}: {exc}", symbol)

            if normalized_records:
                decoded_patterns.append(
                    {
                        "command": command_name,
                        "records": normalized_records,
                    }
                )

        return decoded_patterns

    def _collect_update_invocations(self, item, invocations):
        # SignalR messagepack frames are nested. We walk recursively and collect
        # any invocation matching the shape:
        # [1, {}, None, b"UpdateSomething", payload, ...]
        if isinstance(item, list):
            if len(item) >= 5 and isinstance(item[3], (bytes, bytearray)):
                command_name = bytes(item[3])
                if command_name.startswith(b"Update"):
                    invocations.append(item)
            for child in item:
                self._collect_update_invocations(child, invocations)
            return

        if isinstance(item, dict):
            for key, value in item.items():
                self._collect_update_invocations(key, invocations)
                self._collect_update_invocations(value, invocations)

    def _decode_by_markers(self, raw_bytes: bytes, symbol: str):
        decoded_patterns = []

        for marker, command_name in self._command_markers.items():
            start = 0
            while True:
                idx = raw_bytes.find(marker, start)
                if idx == -1:
                    break

                payload = raw_bytes[idx + len(marker):]
                records = self._decode_payload_records(payload, command_name, symbol)
                if records:
                    decoded_patterns.append(
                        {
                            "command": command_name,
                            "records": records,
                        }
                    )
                start = idx + len(marker)

        return decoded_patterns

    def _decode_payload_records(self, payload: bytes, command_name: str, symbol: str):
        unpacker = msgpack.Unpacker(raw=True, strict_map_key=False)
        unpacker.feed(payload)

        try:
            first_obj = next(unpacker)
        except StopIteration:
            return []
        except Exception as exc:
            log_data("Payload decode error", f"{command_name}: {exc}", symbol)
            return []

        records = first_obj[0] if isinstance(first_obj, list) and first_obj and isinstance(first_obj[0], list) else []
        decoder = self._decoders.get(command_name)
        if not decoder:
            return []

        normalized_records = []
        for record in records:
            try:
                normalized_records.append(decoder(record))
            except Exception as exc:
                log_data("Decode record error", f"{command_name}: {exc}", symbol)
        return normalized_records

    def _decode_update_last_prices(self, record):
        # Confirmed mapping from live samples:
        # - record[7]  -> active_buy_volume
        # - record[9]  -> total_buy_sale_volume
        # - active_sell_volume is derived and does not come directly from raw
        #
        # TODO: once tick-data format is finalized, derive/store a tick snapshot
        # from this pattern using:
        # - event_time
        # - last_price
        # - last_volume
        # - reference_price from UpdateRefPrices
        total_buy_sale_volume = self._to_float(record[9])
        active_buy_volume = self._to_float(record[7])
        active_sell_volume = total_buy_sale_volume - active_buy_volume

        return {
            "symbol": self._decode_symbol(record[0]),
            "event_time": self._to_iso(record[1]),
            "last_price": self._to_float(record[2]),
            "last_volume": self._to_float(record[3]),
            "total_volume": self._to_float(record[4]),
            "total_value": self._to_float(record[5]),
            "matched_price": self._to_float(record[6]),
            "active_buy_volume": active_buy_volume,
            "active_sell_volume": active_sell_volume,
            "total_buy_sale_volume": total_buy_sale_volume,
        }

    def _decode_update_ref_prices(self, record):
        # Reference snapshot used both for ref-price storage and for future
        # tick-data enrichment, especially change/change_percent calculation.
        return {
            "symbol": self._decode_symbol(record[0]),
            "event_time": self._to_iso(record[1]),
            "reference_price": self._to_float(record[2]),
            "ceiling_price": self._to_float(record[3]),
            "floor_price": self._to_float(record[4]),
            "open_price": self._to_float(record[5]),
            "high_price": self._to_float(record[6]),
            "low_price": self._to_float(record[7]),
            "market_status": record[8],
            "trading_date": self._to_iso(record[9]),
        }

    def _decode_update_order_books(self, record):
        # Order-book snapshot:
        # - record[4] is bid side (best bid first)
        # - record[5] is ask side (best ask first)
        #
        # TODO: if needed later, extend each level with side/level_no metadata.
        return {
            "symbol": self._decode_symbol(record[0]),
            "event_time": self._to_iso(record[1]),
            "total_bid_volume": self._to_float(record[2]),
            "total_ask_volume": self._to_float(record[3]),
            "bid_levels": [self._decode_book_level(level) for level in record[4]],
            "ask_levels": [self._decode_book_level(level) for level in record[5]],
        }

    def _decode_update_foreigner_stats(self, record):
        return {
            "symbol": self._decode_symbol(record[0]),
            "event_time": self._to_iso(record[1]),
            "net_volume": self._to_float(record[2]),
            "buy_volume": self._to_float(record[3]),
            "sell_volume": self._to_float(record[4]),
            "buy_value": self._to_float(record[5]),
            "sell_value": self._to_float(record[6]),
        }

    def _decode_update_symbols(self, record):
        return {
            "symbol": self._decode_symbol(record[0]),
            "instrument_type": self._decode_text(record[1]),
            "display_name": self._decode_text(record[2]),
            "exchange": self._decode_text(record[3]),
            "is_active": bool(record[4]),
            "lot_size": self._decode_text(record[5]),
            "reference_code": self._decode_text(record[6]),
            "sector": self._decode_text(record[7]),
            "industry": self._decode_text(record[8]),
            "multiplier": self._to_float(record[9]),
            "market_code": record[10],
            "trading_session": self._decode_text(record[11]),
            "timezone": self._decode_text(record[12]),
            "tradable": bool(record[13]),
            "marginable": bool(record[14]),
            "status_code": record[15],
            "market_cap_1": self._to_float(record[16]),
            "market_cap_2": self._to_float(record[17]),
            "industry_code": self._decode_text(record[18]),
        }

    def _decode_book_level(self, level):
        return {
            "price": self._to_float(level[0]),
            "volume": self._to_float(level[1]),
        }

    def _decode_symbol(self, value):
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        return str(value)

    def _decode_text(self, value):
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        return str(value)

    def _to_float(self, value):
        return None if value is None else float(value)

    def _to_iso(self, value):
        if hasattr(value, "seconds") and hasattr(value, "nanoseconds"):
            ts = value.seconds + (value.nanoseconds / 1_000_000_000)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        return value

import threading
import time

import ssl
import websocket

from config_loader import build_subscribe_trades_command_base64
from parser.message_router import route_binary_message
from settings import PING_INTERVAL_SEC, PING_TIMEOUT_SEC, RECONNECT_DELAY_SEC, RS, WS_URL
from utils.logging_utils import log_data, log_step
from ws.client import APEG_RAW, decode_command_b64, send_apeg, send_handshake

HANDSHAKE_ACK = "{}" + RS
HANDSHAKE_ACK_RAW = HANDSHAKE_ACK.encode("utf-8")


class SymbolSession:
    def __init__(self, symbol: str, command_b64: str):
        self.symbol = symbol
        self.command_b64 = command_b64
        self.quotes_command_raw = decode_command_b64(command_b64)
        self.trades_command_b64 = build_subscribe_trades_command_base64(symbol)
        self.trades_command_raw = decode_command_b64(self.trades_command_b64)

    def create_app(self):
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.symbol = self.symbol
        ws.handshake_done = False
        ws.subscribed = False
        ws.awaiting_apeg_pong = False
        ws.last_ping_sent_at = None
        ws.last_pong_received_at = None
        return ws

    def on_open(self, ws):
        log_step("WebSocket connected", self.symbol)
        send_handshake(ws)

        ping_thread = threading.Thread(
            target=self.app_ping_loop,
            args=(ws,),
            daemon=True,
            name=f"ping-{self.symbol}",
        )
        ping_thread.start()

    def on_message(self, ws, message):
        if isinstance(message, str):
            log_step("Text frame received", self.symbol)
            log_data("Text", repr(message), self.symbol)
            if not ws.handshake_done and message.strip() == HANDSHAKE_ACK:
                self.finish_handshake(ws)
            return

        msg_bytes = message
        log_step("Binary frame received", self.symbol)

        if not ws.handshake_done and msg_bytes == HANDSHAKE_ACK_RAW:
            self.finish_handshake(ws)
            return

        if msg_bytes == APEG_RAW:
            log_step("Received application pong: ApEG", self.symbol)
            ws.last_pong_received_at = time.time()
            ws.awaiting_apeg_pong = False
            return

        route_binary_message(self.symbol, msg_bytes)

    def on_error(self, ws, error):
        log_step("Socket error", self.symbol)
        log_data("Error detail", error, self.symbol)

    def on_close(self, ws, code, reason):
        log_step("WebSocket closed", self.symbol)
        log_data("Close code", code, self.symbol)
        log_data("Reason", reason, self.symbol)

    def send_subscribe_command(self, ws):
        log_step("Send subscribe quotes command", self.symbol)
        ws.send(self.quotes_command_raw, opcode=websocket.ABNF.OPCODE_BINARY)
        log_step("Subscribe quotes completed", self.symbol)

        log_step("Send subscribe trades command", self.symbol)
        ws.send(self.trades_command_raw, opcode=websocket.ABNF.OPCODE_BINARY)
        ws.subscribed = True
        log_step("Subscribe completed", self.symbol)

    def finish_handshake(self, ws):
        ws.handshake_done = True
        log_step("Handshake response OK from server", self.symbol)
        send_apeg(ws, as_ping=False)
        self.send_subscribe_command(ws)

    def app_ping_loop(self, ws):
        while getattr(ws, "keep_running", False):
            try:
                if not getattr(ws, "handshake_done", False):
                    time.sleep(1)
                    continue

                now = time.time()
                if ws.awaiting_apeg_pong and ws.last_ping_sent_at is not None:
                    waited = now - ws.last_ping_sent_at
                    if waited > PING_TIMEOUT_SEC:
                        log_step("Ping timeout", self.symbol)
                        log_data("Timeout after seconds", waited, self.symbol)
                        ws.close()
                        break

                should_send_ping = (
                    not ws.awaiting_apeg_pong
                    and (ws.last_ping_sent_at is None or (now - ws.last_ping_sent_at) >= PING_INTERVAL_SEC)
                )
                if should_send_ping:
                    send_apeg(ws, as_ping=True)
                time.sleep(1)
            except Exception as exc:
                log_step("app_ping_loop error", self.symbol)
                log_data("Error detail", str(exc), self.symbol)
                time.sleep(1)

    def run_forever(self):
        while True:
            ws = self.create_app()
            try:
                ws.run_forever(
                    sslopt={"cert_reqs": ssl.CERT_NONE},
                    ping_interval=0,
                    ping_timeout=None,
                )
            except Exception as exc:
                log_step("run_forever error", self.symbol)
                log_data("Error detail", str(exc), self.symbol)

            log_step(f"Reconnect after {RECONNECT_DELAY_SEC}s", self.symbol)
            time.sleep(RECONNECT_DELAY_SEC)


def run_symbol_worker(symbol: str, command_map) -> None:
    command_b64 = command_map.get(symbol)
    if not command_b64:
        raise ValueError(f"Missing command mapping for symbol: {symbol}")

    SymbolSession(symbol, command_b64).run_forever()

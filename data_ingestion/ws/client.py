import base64
import json

import websocket

from settings import APEG_HEX, RS
from utils.logging_utils import log_data, log_step

APEG_RAW = bytes.fromhex(APEG_HEX)


def send_handshake(ws) -> None:
    handshake = {"protocol": "messagepack", "version": 1}
    text = json.dumps(handshake) + RS
    log_step("Send handshake JSON", getattr(ws, "symbol", None))
    log_data("Handshake", text.encode("utf-8"), getattr(ws, "symbol", None))
    ws.send(text)


def send_apeg(ws, as_ping: bool = False) -> None:
    ws.send(APEG_RAW, opcode=websocket.ABNF.OPCODE_BINARY)
    if as_ping:
        ws.last_ping_sent_at = __import__("time").time()
        ws.awaiting_apeg_pong = True
        log_step("Send application ping: ApEG", getattr(ws, "symbol", None))
    else:
        log_step("Handshake success - send ApEG", getattr(ws, "symbol", None))
    log_data("ApEG raw hex", APEG_RAW.hex(), getattr(ws, "symbol", None))


def decode_command_b64(command_b64: str) -> bytes:
    return base64.b64decode(command_b64)

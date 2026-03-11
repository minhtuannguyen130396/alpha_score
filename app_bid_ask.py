import websocket

from config_loader import load_command_map, load_symbols, validate_symbols
from utils.logging_utils import log_data, log_step
from ws.pool import run_pool


def main() -> None:
    websocket.enableTrace(False)
    command_map = load_command_map()
    symbols = load_symbols()
    validate_symbols(symbols, command_map)

    log_step(f"Loaded {len(symbols)} symbols")
    log_data("Symbols", symbols)
    run_pool(symbols, command_map)


if __name__ == "__main__":
    main()

import argparse

import websocket

from config_loader import load_command_map, validate_symbols
from utils.logging_utils import log_data, log_step
from ws.pool import run_pool


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run bid/ask websocket workers for the provided symbols."
    )
    parser.add_argument(
        "symbols",
        nargs="+",
        help="One or more symbols to subscribe, separated by spaces.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    websocket.enableTrace(False)

    symbols = [symbol.strip().upper() for symbol in args.symbols if symbol.strip()]
    command_map = load_command_map()
    validate_symbols(symbols, command_map)

    log_step(f"Loaded {len(symbols)} symbols from command line")
    log_data("Symbols", symbols)
    run_pool(symbols, command_map)


if __name__ == "__main__":
    main()

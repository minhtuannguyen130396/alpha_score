from __future__ import annotations

import argparse
from pathlib import Path

from layer2.phase0 import Phase0RawSourceIntakeProcess, default_input_root, default_output_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Layer 2 Phase 0 raw source intake poller.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="Polling interval in seconds when running continuously. Default is 5 seconds.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single scan/build cycle and exit.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated symbol list, for example FPT,HPG,MBB.",
    )
    parser.add_argument(
        "--input-root",
        default=str(default_input_root()),
        help="Root folder containing Layer 1 outputs. Defaults to data_ingestion/data.",
    )
    parser.add_argument(
        "--output-root",
        default=str(default_output_root()),
        help="Root folder for Layer 2 phase0 bundles and logs. Defaults to data_processing/data.",
    )
    parser.add_argument(
        "--include-historical-only",
        action="store_true",
        help="Also emit bundles for dates that only have historical_price and no intraday sources.",
    )
    return parser.parse_args()


def parse_symbol_filter(raw_symbols: str | None) -> set[str] | None:
    if not raw_symbols:
        return None
    symbols = {symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()}
    return symbols or None


def main() -> None:
    args = parse_args()
    process = Phase0RawSourceIntakeProcess(
        input_root=Path(args.input_root),
        output_root=Path(args.output_root),
        symbols=parse_symbol_filter(args.symbols),
        include_historical_only=args.include_historical_only,
    )

    if args.run_once:
        process.run_once()
        return

    process.run_forever(interval_seconds=args.interval_seconds)


if __name__ == "__main__":
    main()

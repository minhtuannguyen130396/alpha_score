from __future__ import annotations

import argparse
import time
from pathlib import Path

from layer2.phase0 import Phase0RawSourceIntakeProcess, default_input_root, default_output_root, utc_now_iso
from layer2.phase1 import Phase1SchemaNormalizationProcess
from layer2.phase2 import Phase2InputValidationProcess
from layer2.phase3 import Phase3TimeAlignmentProcess
from layer2.phase4 import Phase4DeepBookReconstructionProcess


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Layer 2 pipeline poller for phase0 through phase4.",
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
        help="Run a single phase0 -> phase4 cycle and exit.",
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
        help="Root folder for Layer 2 outputs. Defaults to data_processing/data.",
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


def stdout(message: str) -> None:
    print(f"[{utc_now_iso()}] {message}")


def run_pipeline_once(args: argparse.Namespace) -> dict[str, dict[str, int]]:
    symbols = parse_symbol_filter(args.symbols)
    output_root = Path(args.output_root)

    phase0 = Phase0RawSourceIntakeProcess(
        input_root=Path(args.input_root),
        output_root=output_root,
        symbols=symbols,
        include_historical_only=args.include_historical_only,
    )
    phase1 = Phase1SchemaNormalizationProcess(
        output_root=output_root,
        symbols=symbols,
    )
    phase2 = Phase2InputValidationProcess(
        output_root=output_root,
        symbols=symbols,
    )
    phase3 = Phase3TimeAlignmentProcess(
        output_root=output_root,
        symbols=symbols,
    )
    phase4 = Phase4DeepBookReconstructionProcess(
        output_root=output_root,
        symbols=symbols,
    )

    phase0_result = phase0.run_once()
    phase1_result = phase1.run_once()
    phase2_result = phase2.run_once()
    phase3_result = phase3.run_once()
    phase4_result = phase4.run_once()
    return {
        "phase0": phase0_result,
        "phase1": phase1_result,
        "phase2": phase2_result,
        "phase3": phase3_result,
        "phase4": phase4_result,
    }


def main() -> None:
    args = parse_args()
    interval_seconds = max(float(args.interval_seconds), 0.1)

    if args.run_once:
        run_pipeline_once(args)
        return

    stdout(
        f"starting layer2 pipeline poller: input={args.input_root} output={args.output_root} interval={interval_seconds}s"
    )
    while True:
        run_pipeline_once(args)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()

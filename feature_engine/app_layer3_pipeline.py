from __future__ import annotations

import argparse
import time
from pathlib import Path

from layer3.phase0 import Phase0Layer2InputIntakeProcess, default_input_root, default_output_root, utc_now_iso
from layer3.phase1 import Phase1MarketStateConstructionProcess
from layer3.phase2 import Phase2BaseFeatureEngineeringProcess
from layer3.phase3 import Phase3FeatureStoreProcess
from layer3.phase4 import Phase4FeatureQualityProcess
from layer3.phase5 import Phase5Layer3OutputPackageProcess


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Layer 3 pipeline poller for phase0 through phase5.",
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
        help="Run a single phase0 -> phase5 cycle and exit.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated symbol list, for example FPT,HPG,MBB.",
    )
    parser.add_argument(
        "--input-root",
        default=str(default_input_root()),
        help="Root folder containing Layer 2 outputs. Defaults to data_processing/data.",
    )
    parser.add_argument(
        "--output-root",
        default=str(default_output_root()),
        help="Root folder for Layer 3 outputs. Defaults to feature_engine/data.",
    )
    return parser.parse_args()


def parse_symbol_filter(raw_symbols: str | None) -> set[str] | None:
    if not raw_symbols:
        return None
    symbols = {symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()}
    return symbols or None


def stdout(message: str) -> None:
    print(f"[{utc_now_iso()}] {message}")


class Layer3Pipeline:
    def __init__(self, args: argparse.Namespace) -> None:
        symbols = parse_symbol_filter(args.symbols)
        self.phase0 = Phase0Layer2InputIntakeProcess(
            input_root=Path(args.input_root),
            output_root=Path(args.output_root),
            symbols=symbols,
        )
        self.phase1 = Phase1MarketStateConstructionProcess(
            output_root=Path(args.output_root),
            symbols=symbols,
        )
        self.phase2 = Phase2BaseFeatureEngineeringProcess(
            output_root=Path(args.output_root),
            symbols=symbols,
        )
        self.phase3 = Phase3FeatureStoreProcess(
            output_root=Path(args.output_root),
            symbols=symbols,
        )
        self.phase4 = Phase4FeatureQualityProcess(
            output_root=Path(args.output_root),
            symbols=symbols,
        )
        self.phase5 = Phase5Layer3OutputPackageProcess(
            output_root=Path(args.output_root),
            symbols=symbols,
        )

    def run_once(self) -> dict[str, dict[str, int]]:
        return {
            "phase0": self.phase0.run_once(),
            "phase1": self.phase1.run_once(),
            "phase2": self.phase2.run_once(),
            "phase3": self.phase3.run_once(),
            "phase4": self.phase4.run_once(),
            "phase5": self.phase5.run_once(),
        }


def main() -> None:
    args = parse_args()
    interval_seconds = max(float(args.interval_seconds), 0.1)
    pipeline = Layer3Pipeline(args)

    if args.run_once:
        pipeline.run_once()
        return

    stdout(
        f"starting layer3 pipeline poller: input={args.input_root} output={args.output_root} interval={interval_seconds}s"
    )
    while True:
        pipeline.run_once()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()

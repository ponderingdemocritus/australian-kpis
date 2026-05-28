#!/usr/bin/env python3
"""Seed cargo-fuzz corpora from committed parser fixtures."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


SEEDS = {
    "sdmx_json": [
        ("crates/adapters/abs/tests/fixtures/cpi_sdmx.json", "cpi_sdmx.json"),
    ],
    "xls": [
        (
            "crates/adapters/rba/tests/fixtures/a1_balance_sheet_weekly.xlsx",
            "a1_balance_sheet_weekly.xlsx",
        ),
        ("crates/adapters/apra/tests/fixtures/centralised.xlsx", "centralised.xlsx"),
    ],
    "csv": [
        (
            "crates/adapters/rba/tests/fixtures/g1_consumer_price_inflation.csv",
            "g1_consumer_price_inflation.csv",
        ),
    ],
    "pdf_response": [
        ("fuzz/corpus/pdf_response/sidecar-response.json", "sidecar-response.json"),
    ],
}


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=repo_root,
        help="repository root containing parser fixtures",
    )
    parser.add_argument(
        "--fuzz-dir",
        type=Path,
        default=repo_root / "fuzz",
        help="cargo-fuzz directory whose corpus/ subdirectories will be seeded",
    )
    return parser.parse_args()


def copy_seed(repo_root: Path, fuzz_dir: Path, target: str, source: str, filename: str) -> None:
    source_path = repo_root / source
    destination = fuzz_dir / "corpus" / target / filename
    if not source_path.is_file():
        raise FileNotFoundError(f"missing fuzz seed fixture: {source_path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source_path.resolve() == destination.resolve():
        return
    shutil.copyfile(source_path, destination)


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    fuzz_dir = args.fuzz_dir.resolve()

    for target, seeds in SEEDS.items():
        for source, filename in seeds:
            copy_seed(repo_root, fuzz_dir, target, source, filename)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

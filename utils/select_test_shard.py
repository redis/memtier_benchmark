#!/usr/bin/env python3
"""Select a deterministic file shard using RLTest's default discovery pattern."""

import argparse
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", type=int, help="one-based shard number")
    parser.add_argument("count", type=int, help="total number of shards")
    args = parser.parse_args()
    if not 1 <= args.shard <= args.count:
        parser.error("require 1 <= shard <= count")

    # RLTest scans only test*.py in the tests directory, without recursion.
    # Keep files intact so class fixtures and module-local ordering are retained.
    test_dir = Path(__file__).resolve().parents[1] / "tests"
    files = sorted(path.name for path in test_dir.glob("test*.py"))
    selected = files[args.shard - 1::args.count]
    if not selected:
        parser.error("shard is empty; refusing to fall back to the full suite")
    print(f"Shard {args.shard}/{args.count}: {len(selected)}/{len(files)} files",
          file=sys.stderr)
    # tests/run_tests.sh accepts whitespace-separated file names in TEST.
    print(" ".join(selected))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Check that file shards collect every default RLTest test exactly once."""

import argparse
from collections import Counter
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("count", type=int)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("count must be positive")
    root = Path(__file__).resolve().parents[1]

    def collect(files=()):
        command = [sys.executable, "-m", "RLTest", "--collect-only"]
        for name in files:
            command.extend(["--test", name])
        result = subprocess.run(command, cwd=root / "tests", check=True,
                                text=True, stdout=subprocess.PIPE)
        return result.stdout.splitlines()

    expected = Counter(collect())
    if not expected:
        raise RuntimeError("RLTest collected no tests")
    actual = Counter()
    for shard in range(1, args.count + 1):
        result = subprocess.run(
            [sys.executable, str(root / "utils/select_test_shard.py"),
             str(shard), str(args.count)],
            check=True, text=True, stdout=subprocess.PIPE)
        tests = collect(result.stdout.split())
        if not tests:
            raise RuntimeError(f"Shard {shard} collected no tests")
        actual.update(tests)
        print(f"Shard {shard}: {len(tests)} tests")
    if actual != expected or any(count != 1 for count in actual.values()):
        raise RuntimeError(
            f"Shard mismatch: missing={expected - actual}, extra={actual - expected}")
    print(f"All {len(actual)} tests are assigned exactly once.")


if __name__ == "__main__":
    main()

"""
starter_lab1.py — STARTER VERSION (Student Starter Code)

Module 1 Lab: Streaming Data Pipeline & Structured Execution

You will implement:
- Streaming file reader (generator)
- Lazy filter stage (generator)
- Parsing stage to structured dict records (generator)
- Pipeline composition + summary statistics
- Class-based context manager for timing and exception logging

Rules / Constraints:
- Do NOT use f.readlines() in read_lines()
- Generator stages must remain lazy (do not materialize lists)
- parse_line() must support messages with spaces
- ExecutionTimer must *not* suppress exceptions

How to run (after implementing):
    python starter_lab1.py --path app.log --preview 3
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


class ExecutionTimer:
    """
    TODO: Implement a class-based context manager.

    Requirements:
    - Record start time in __enter__
    - Print elapsed time in __exit__
    - Log exceptions if they occur
    - Propagate exceptions (do not suppress)
    """

    def __init__(self, label: str = "ExecutionTimer") -> None:
        self.label = label
        self._start: Optional[float] = None

    def __enter__(self) -> "ExecutionTimer":
        # TODO: record start time (recommend time.perf_counter)
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # TODO: print elapsed time
        # TODO: if exc_type is not None, log exception details
        # TODO: return False to propagate exceptions
        raise NotImplementedError


def read_lines(path: str | Path) -> Iterator[str]:
    """
    TODO: Streaming reader that yields one stripped line at a time.

    Constraints:
    - Uses 'with open(...)'
    - Does NOT use readlines()
    - Does NOT load entire file into memory
    """
    # TODO: implement
    raise NotImplementedError


def filter_errors(lines: Iterable[str]) -> Iterator[str]:
    """
    TODO: Lazy filter stage: yields only lines containing 'ERROR'.

    Accepts any iterable of strings and remains lazy.
    """
    # TODO: implement
    raise NotImplementedError


def parse_line(line: str) -> Dict[str, str]:
    """
    TODO: Parse a single log line into a dict with:
        level, date, message

    Expected format:
        'LEVEL YYYY-MM-DD message...'

    Requirements:
    - Message may contain spaces
    - If malformed, raise ValueError
    """
    # Hint: line.split(maxsplit=2)
    raise NotImplementedError


def parse_lines(lines: Iterable[str]) -> Iterator[Dict[str, str]]:
    """
    TODO: Lazy parse stage: yields structured records as dictionaries:
        {"level": "...", "date": "...", "message": "..."}
    """
    # TODO: implement
    raise NotImplementedError


def build_pipeline(path: str | Path) -> Iterator[Dict[str, str]]:
    """Compose the required lazy pipeline (do NOT materialize lists)."""
    return parse_lines(filter_errors(read_lines(path)))


def summarize_errors(records: Iterable[Dict[str, str]]) -> Tuple[int, List[str], Counter]:
    """
    Compute required summary outputs:
    - Total number of error records
    - Unique error dates (sorted)
    - Count of errors per date (Counter)
    """
    total = 0
    per_date: Counter = Counter()

    for rec in records:
        total += 1
        per_date[rec["date"]] += 1

    unique_dates = sorted(per_date.keys())
    return total, unique_dates, per_date


def generate_sample_log(path: str | Path, repeat: int = 1000) -> None:
    """
    Create a sample log file for testing.
    repeat controls the number of times the sample block is repeated.
    """
    path = Path(path)
    sample_block = [
        "INFO 2026-02-01 User login successful",
        "ERROR 2026-02-01 Database connection failed",
        "INFO 2026-02-01 Scheduled job started",
        "ERROR 2026-02-02 Timeout during API call",
        "INFO 2026-02-02 Health check passed",
        "ERROR 2026-02-03 Invalid credentials provided",
    ]

    with path.open("w", encoding="utf-8") as f:
        for _ in range(repeat):
            for line in sample_block:
                f.write(line + "\n")

    print(
        f"Generated sample log at {path} with repeat={repeat} "
        f"({repeat * len(sample_block)} lines)."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Module 1 Lab (starter)")
    parser.add_argument("--path", default="app.log", help="Path to log file (default: app.log)")
    parser.add_argument("--preview", type=int, default=0, help="Print first N parsed ERROR records.")
    parser.add_argument("--generate-sample", action="store_true", help="Generate a sample app.log file.")
    parser.add_argument("--repeat", type=int, default=1000, help="Repeat count for sample generation.")
    args = parser.parse_args()

    path = Path(args.path)

    if args.generate_sample:
        generate_sample_log(path, repeat=max(1, args.repeat))

    if not path.exists():
        print(f"ERROR: File not found: {path}")
        print("Tip: run with --generate-sample to create one.")
        return 2

    # Run the pipeline with a timer (once implemented)
    with ExecutionTimer("Pipeline run"):
        records = build_pipeline(path)

        if args.preview > 0:
            print(f"\nPreviewing first {args.preview} ERROR records:")
            for i, rec in enumerate(records, start=1):
                print(rec)
                if i >= args.preview:
                    break

            # IMPORTANT: generators are single-use; rebuild for summary
            records = build_pipeline(path)

        total, unique_dates, per_date = summarize_errors(records)

    print("\n=== Summary ===")
    print(f"Total ERROR records: {total}")
    print(f"Unique ERROR dates: {unique_dates}")
    print("Errors per date:")
    for date, count in per_date.items():
        print(f"  {date}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

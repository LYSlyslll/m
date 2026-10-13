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
import gc
import logging
import time
import tracemalloc
from collections import Counter
from datetime import date
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
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        elapsed = time.perf_counter() - self._start
        print(f"{self.label}: {elapsed:.6f} seconds")
        if exc_type is not None:
            logging.error(
                "%s failed: %s: %s",
                self.label,
                exc_type.__name__,
                exc_val,
                exc_info=(exc_type, exc_val, exc_tb),
            )
        return False


def read_lines(path: str | Path) -> Iterator[str]:
    """
    TODO: Streaming reader that yields one stripped line at a time.

    Constraints:
    - Uses 'with open(...)'
    - Does NOT use readlines()
    - Does NOT load entire file into memory
    """
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield line.strip()


def filter_errors(lines: Iterable[str]) -> Iterator[str]:
    """
    TODO: Lazy filter stage: yields only lines containing 'ERROR'.

    Accepts any iterable of strings and remains lazy.
    """
    for line in lines:
        if "ERROR" in line:
            yield line


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
    parts = line.split(maxsplit=2)
    if len(parts) != 3 or not parts[2].strip():
        raise ValueError(f"Malformed log line: {line!r}")
    level, log_date, message = parts
    try:
        parsed_date = date.fromisoformat(log_date)
    except ValueError as exc:
        raise ValueError(f"Malformed log date: {log_date!r}") from exc
    if parsed_date.isoformat() != log_date:
        raise ValueError(f"Expected YYYY-MM-DD date: {log_date!r}")
    return {"level": level, "date": log_date, "message": message}


def parse_lines(lines: Iterable[str]) -> Iterator[Dict[str, str]]:
    """
    TODO: Lazy parse stage: yields structured records as dictionaries:
        {"level": "...", "date": "...", "message": "..."}
    """
    for line in lines:
        yield parse_line(line)


def build_pipeline(path: str | Path) -> Iterator[Dict[str, str]]:
    """Version A: chain lazy generators without materializing lists."""
    return parse_lines(filter_errors(read_lines(path)))


def build_pipeline_eager(path: str | Path) -> List[Dict[str, str]]:
    """Version B: intentionally waste memory by materializing every stage.

    This comparison violates the lab's streaming design constraint and should
    not be used as the normal pipeline.
    """
    lines = list(read_lines(path))
    errors = list(filter_errors(lines))
    records = list(parse_lines(errors))
    return records


def compare_pipeline_memory(path: str | Path) -> Dict[str, int]:
    """Measure peak Python allocations for construction AND full consumption.

    Each version gets a separate tracing session; sample generation is excluded.
    Results are approximate allocation peaks, not whole-process resident memory.
    """
    if tracemalloc.is_tracing():
        raise RuntimeError("Memory comparison requires its own tracemalloc session")
    peaks: Dict[str, int] = {}
    expected_summary = None
    for name, builder in [("lazy", build_pipeline), ("eager", build_pipeline_eager)]:
        gc.collect()
        tracemalloc.start()
        try:
            summary = summarize_errors(builder(path))
            _, peaks[name] = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        if expected_summary is None:
            expected_summary = summary
        elif summary != expected_summary:
            raise AssertionError("Lazy and eager summaries differ")
        del summary
    return peaks


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
    parser.add_argument(
        "--compare-memory", action="store_true",
        help="Compare peak Python memory for lazy and eager pipelines on the same log.",
    )
    args = parser.parse_args()

    path = Path(args.path)

    if args.generate_sample:
        generate_sample_log(path, repeat=max(1, args.repeat))

    if not path.exists():
        print(f"ERROR: File not found: {path}")
        print("Tip: run with --generate-sample to create one.")
        return 2

    if args.compare_memory:
        peaks = compare_pipeline_memory(path)
        mib = 1024 * 1024
        print("Peak traced Python allocations (approximate; full pipeline and summary):")
        print(f"Version A (lazy):  {peaks['lazy'] / mib:.3f} MiB")
        print(f"Version B (eager): {peaks['eager'] / mib:.3f} MiB")
        print(f"Extra eager memory: {(peaks['eager'] - peaks['lazy']) / mib:.3f} MiB")
        if peaks['lazy']:
            print(f"Eager/lazy peak ratio: {peaks['eager'] / peaks['lazy']:.1f}x")
        print("Both versions produced identical summaries.")
        return 0

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

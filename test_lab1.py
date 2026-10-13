import contextlib
import importlib.util
import io
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "lab", Path(__file__).with_name("starter_lab1-1.py")
)
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


class LabTests(unittest.TestCase):
    def test_eager_and_lazy_results_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "app.log"
            path.write_text("INFO 2026-02-01 ok\nERROR 2026-02-02 two words\n", encoding="utf-8")
            self.assertEqual(list(lab.build_pipeline(path)), lab.build_pipeline_eager(path))

    def test_memory_comparison_consumes_both_versions_and_stops_tracing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "app.log"
            path.write_text("ERROR 2026-02-01 message\n" * 2000, encoding="utf-8")
            peaks = lab.compare_pipeline_memory(path)
            self.assertGreater(peaks["lazy"], 0)
            self.assertGreater(peaks["eager"], peaks["lazy"])
            self.assertFalse(lab.tracemalloc.is_tracing())
            path.write_text("ERROR malformed\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                lab.compare_pipeline_memory(path)
            self.assertFalse(lab.tracemalloc.is_tracing())

    def test_timer_records_elapsed_time_and_returns_self(self):
        output = io.StringIO()
        timer = lab.ExecutionTimer("test")
        with patch.object(lab.time, "perf_counter", side_effect=[10.0, 10.25]):
            with contextlib.redirect_stdout(output):
                with timer as entered:
                    self.assertIs(entered, timer)
        self.assertIn("test: 0.250000 seconds", output.getvalue())

    def test_timer_logs_and_propagates_original_exception(self):
        error = ValueError("broken record")
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(ValueError) as caught:
                with lab.ExecutionTimer("pipeline"):
                    raise error
        self.assertIs(caught.exception, error)
        self.assertIn("pipeline", output.getvalue())
        self.assertIn("ValueError: broken record", logs.output[0])

    def test_reader_is_lazy_and_strips_each_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "app.log"
            lines = lab.read_lines(path)
            path.write_text("  first  \n\nsecond\n", encoding="utf-8")
            self.assertEqual(list(lines), ["first", "", "second"])

    def test_stages_are_lazy_and_accept_single_use_iterables(self):
        consumed = []

        def source():
            for line in ["INFO 2026-02-01 ok", "ERROR 2026-02-02 two words", "bad ERROR"]:
                consumed.append(line)
                yield line

        records = lab.parse_lines(lab.filter_errors(source()))
        self.assertEqual(consumed, [])
        self.assertEqual(next(records)["message"], "two words")
        self.assertEqual(len(consumed), 2)
        with self.assertRaises(ValueError):
            next(records)

    def test_filter_uses_error_substring(self):
        self.assertEqual(
            list(lab.filter_errors(iter(["INFO ERROR detail", "ERROR", "error", "INFO"]))),
            ["INFO ERROR detail", "ERROR"],
        )

    def test_parse_preserves_message_spaces(self):
        self.assertEqual(
            lab.parse_line("ERROR 2026-02-01 Database  connection failed"),
            {"level": "ERROR", "date": "2026-02-01", "message": "Database  connection failed"},
        )

    def test_parse_rejects_malformed_records(self):
        for line in ["", "ERROR", "ERROR 2026-02-01", "ERROR 2026-02-01   ",
                     "ERROR invalid message", "ERROR 2026-02-30 message",
                     "ERROR 20260201 message"]:
            with self.subTest(line=line), self.assertRaises(ValueError):
                lab.parse_line(line)

    def test_sample_pipeline_and_empty_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "app.log"
            with contextlib.redirect_stdout(io.StringIO()):
                lab.generate_sample_log(path, repeat=2)
            self.assertEqual(
                lab.summarize_errors(lab.build_pipeline(path)),
                (6, ["2026-02-01", "2026-02-02", "2026-02-03"],
                 Counter({"2026-02-01": 2, "2026-02-02": 2, "2026-02-03": 2})),
            )
        self.assertEqual(lab.summarize_errors(iter(())), (0, [], Counter()))


if __name__ == "__main__":
    unittest.main()

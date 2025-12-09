#! /usr/bin/env python3

"""Integration tests for nomino_mapper.py"""

import subprocess
import sys
import tempfile
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_INPUT = FIXTURES_DIR / "sample_input.csv"
EXPECTED_OUTPUT = FIXTURES_DIR / "expected_output.jsonl"
MAPPER_SCRIPT = Path(__file__).parent.parent / "src" / "nomino_mapper.py"


def test_mapper_produces_expected_output():
    """Test that mapper produces expected output from sample input."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        output_path = f.name

    try:
        result = subprocess.run(
            [sys.executable, str(MAPPER_SCRIPT), "-i", str(SAMPLE_INPUT), "-o", output_path],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, f"Mapper failed: {result.stderr}"

        with open(output_path, "r", encoding="utf-8") as f:
            actual = f.read()
        with open(EXPECTED_OUTPUT, "r", encoding="utf-8") as f:
            expected = f.read()

        assert actual == expected, f"Output mismatch:\nGot:\n{actual}\nExpected:\n{expected}"

    finally:
        Path(output_path).unlink(missing_ok=True)

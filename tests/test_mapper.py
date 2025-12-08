#! /usr/bin/env python3

"""Integration tests for nomino_mapper.py"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_INPUT = FIXTURES_DIR / "sample_input.csv"
EXPECTED_OUTPUT = FIXTURES_DIR / "expected_output.jsonl"
MAPPER_SCRIPT = Path(__file__).parent.parent / "src" / "nomino_mapper.py"


def load_jsonl(filepath: Path) -> list[dict]:
    """Load a JSONL file into a list of dicts."""
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    return records


def normalize_record(record: dict) -> dict:
    """Normalize a record for comparison (sort features list)."""
    normalized = dict(record)
    if "FEATURES" in normalized:
        normalized["FEATURES"] = sorted(
            normalized["FEATURES"], key=lambda x: json.dumps(x, sort_keys=True)
        )
    return normalized


def test_mapper_produces_expected_output():
    """Test that mapper produces expected output from sample input."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        output_path = f.name

    try:
        result = subprocess.run(
            [sys.executable, str(MAPPER_SCRIPT), "-i", str(SAMPLE_INPUT), "-o", output_path],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"Mapper failed: {result.stderr}"

        actual_records = load_jsonl(Path(output_path))
        expected_records = load_jsonl(EXPECTED_OUTPUT)

        assert len(actual_records) == len(expected_records), (
            f"Record count mismatch: got {len(actual_records)}, expected {len(expected_records)}"
        )

        actual_by_id = {r["RECORD_ID"]: normalize_record(r) for r in actual_records}
        expected_by_id = {r["RECORD_ID"]: normalize_record(r) for r in expected_records}

        assert set(actual_by_id.keys()) == set(expected_by_id.keys()), (
            f"RECORD_ID mismatch: got {set(actual_by_id.keys())}, expected {set(expected_by_id.keys())}"
        )

        for record_id in expected_by_id:
            actual = actual_by_id[record_id]
            expected = expected_by_id[record_id]
            assert actual == expected, (
                f"Record {record_id} mismatch:\n"
                f"Got: {json.dumps(actual, indent=2)}\n"
                f"Expected: {json.dumps(expected, indent=2)}"
            )

    finally:
        Path(output_path).unlink(missing_ok=True)


def test_mapper_output_has_required_fields():
    """Test that all output records have required Senzing fields."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        output_path = f.name

    try:
        subprocess.run(
            [sys.executable, str(MAPPER_SCRIPT), "-i", str(SAMPLE_INPUT), "-o", output_path],
            capture_output=True,
            text=True,
        )

        records = load_jsonl(Path(output_path))

        for record in records:
            assert "DATA_SOURCE" in record, f"Missing DATA_SOURCE in {record.get('RECORD_ID')}"
            assert "RECORD_ID" in record, "Missing RECORD_ID"
            assert "FEATURES" in record, f"Missing FEATURES in {record.get('RECORD_ID')}"
            assert isinstance(record["FEATURES"], list), "FEATURES must be a list"
            assert len(record["FEATURES"]) > 0, "FEATURES must not be empty"

    finally:
        Path(output_path).unlink(missing_ok=True)


def test_mapper_features_have_record_type():
    """Test that all records have RECORD_TYPE in FEATURES."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        output_path = f.name

    try:
        subprocess.run(
            [sys.executable, str(MAPPER_SCRIPT), "-i", str(SAMPLE_INPUT), "-o", output_path],
            capture_output=True,
            text=True,
        )

        records = load_jsonl(Path(output_path))

        for record in records:
            record_types = [f.get("RECORD_TYPE") for f in record["FEATURES"] if "RECORD_TYPE" in f]
            assert len(record_types) == 1, f"Expected exactly one RECORD_TYPE in {record.get('RECORD_ID')}"
            assert record_types[0] in ("PERSON", "ORGANIZATION", "VESSEL", "AIRCRAFT"), (
                f"Invalid RECORD_TYPE {record_types[0]} in {record.get('RECORD_ID')}"
            )

    finally:
        Path(output_path).unlink(missing_ok=True)

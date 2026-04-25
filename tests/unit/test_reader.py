"""
Unit tests for src/ingestion/reader.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from ingestion.reader import (
    _sorted_partition_paths,
    read_partitions_to_parquet,
    validate_raw_inputs,
)

from tests.conftest import VALID_ROW


# ---------------------------------------------------------------------------
# _sorted_partition_paths
# ---------------------------------------------------------------------------

class TestSortedPartitionPaths:
    def test_returns_sorted_list(self, tmp_path):
        raw = tmp_path / "raw"
        raw.mkdir()
        for name in ["part_003.csv", "part_001.csv", "part_002.csv"]:
            (raw / name).write_text("x")
        paths = _sorted_partition_paths(raw)
        assert [p.name for p in paths] == ["part_001.csv", "part_002.csv", "part_003.csv"]

    def test_raises_when_no_files(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(FileNotFoundError):
            _sorted_partition_paths(empty)


# ---------------------------------------------------------------------------
# validate_raw_inputs
# ---------------------------------------------------------------------------

class TestValidateRawInputs:
    def test_valid_directory(self, tmp_raw_dir):
        summary = validate_raw_inputs(tmp_raw_dir)
        assert summary["total_files"] >= 1

    def test_raises_on_empty_directory(self, tmp_path):
        empty = tmp_path / "raw"
        empty.mkdir()
        with pytest.raises(FileNotFoundError):
            validate_raw_inputs(empty)

    def test_raises_on_empty_file(self, tmp_path):
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "part_001.csv").write_bytes(b"")
        with pytest.raises(ValueError):
            validate_raw_inputs(raw)


# ---------------------------------------------------------------------------
# read_partitions_to_parquet
# ---------------------------------------------------------------------------

class TestReadPartitionsToParquet:
    def test_row_count_matches(self, tmp_raw_dir, tmp_path):
        out = tmp_path / "out.parquet"
        meta = read_partitions_to_parquet(tmp_raw_dir, out)
        assert meta["row_count"] == 1
        assert out.exists()

    def test_metadata_keys(self, tmp_raw_dir, tmp_path):
        meta = read_partitions_to_parquet(tmp_raw_dir, tmp_path / "out.parquet")
        assert {"row_count", "file_count", "columns"} <= set(meta.keys())

    def test_null_value_variants(self, tmp_path):
        """Various null representations must land as NULL (None) in Parquet."""
        raw = tmp_path / "raw"
        raw.mkdir()
        header = ",".join(VALID_ROW.keys())
        # Replace amount and risk_score with null-like strings
        row = dict(VALID_ROW)
        row["amount"] = "N/A"
        row["risk_score"] = "none"
        values = ",".join(str(v) for v in row.values())
        (raw / "part_001.csv").write_text(f"{header}\n{values}\n")

        import pyarrow.parquet as pq
        out = tmp_path / "out.parquet"
        read_partitions_to_parquet(raw, out)
        table = pq.read_table(out)
        amount_col = table.column("amount")
        risk_col   = table.column("risk_score")
        assert amount_col[0].as_py() is None
        assert risk_col[0].as_py() is None

    def test_multi_partition_concat(self, tmp_path):
        """Two partitions must be concatenated correctly."""
        raw = tmp_path / "raw"
        raw.mkdir()
        header = ",".join(VALID_ROW.keys())
        row_str = ",".join(str(v) for v in VALID_ROW.values())
        (raw / "part_001.csv").write_text(f"{header}\n{row_str}\n")
        row2 = dict(VALID_ROW)
        row2["amount"] = 200.0
        row2_str = ",".join(str(v) for v in row2.values())
        (raw / "part_002.csv").write_text(f"{row2_str}\n")

        meta = read_partitions_to_parquet(raw, tmp_path / "out.parquet")
        assert meta["row_count"] == 2
        assert meta["file_count"] == 2

    def test_raises_on_missing_columns(self, tmp_path):
        """CSV without required columns must raise ValueError."""
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "part_001.csv").write_text("col_a,col_b\n1,2\n")
        with pytest.raises(ValueError, match="Missing expected columns"):
            read_partitions_to_parquet(raw, tmp_path / "out.parquet")

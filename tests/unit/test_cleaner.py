"""
Unit tests for src/cleaning/cleaner.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from cleaning.cleaner import clean_transactions
from tests.conftest import RAW_SCHEMA, VALID_ROW


def _write_raw(tmp_path: Path, rows: list[dict]) -> Path:
    """Helper: write rows as raw Parquet using RAW_SCHEMA."""
    if not rows:
        table = pa.table(
            {name: pa.array([], type=field.type) for name, field in zip(RAW_SCHEMA.names, RAW_SCHEMA)},
            schema=RAW_SCHEMA,
        )
    else:
        arrays = {
            name: pa.array([r.get(name) for r in rows], type=field.type)
            for name, field in zip(RAW_SCHEMA.names, RAW_SCHEMA)
        }
        table = pa.table(arrays, schema=RAW_SCHEMA)
    out = tmp_path / "raw.parquet"
    pq.write_table(table, out)
    return out


class TestCleanTransactionsMetricKeys:
    def test_returns_expected_keys(self, tmp_path):
        raw = _write_raw(tmp_path, [VALID_ROW])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert {"total_raw", "total_cleaned", "total_dropped",
                "duplicates_removed", "null_critical_dropped",
                "invalid_amount_dropped"} <= set(metrics.keys())


class TestCleanDeduplication:
    def test_removes_exact_duplicates(self, tmp_path):
        raw = _write_raw(tmp_path, [VALID_ROW, VALID_ROW, VALID_ROW])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1
        assert metrics["duplicates_removed"] == 2

    def test_keeps_distinct_rows(self, tmp_path):
        row2 = dict(VALID_ROW); row2["amount"] = 999.0
        raw = _write_raw(tmp_path, [VALID_ROW, row2])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 2


class TestCleanNullCritical:
    def test_drops_null_amount(self, tmp_path):
        bad = dict(VALID_ROW); bad["amount"] = None
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1

    def test_drops_null_location_region(self, tmp_path):
        bad = dict(VALID_ROW); bad["location_region"] = None
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1

    def test_drops_null_anomaly(self, tmp_path):
        bad = dict(VALID_ROW); bad["anomaly"] = None
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1


class TestCleanInvalidAmount:
    def test_drops_zero_amount(self, tmp_path):
        bad = dict(VALID_ROW); bad["amount"] = 0.0
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1

    def test_drops_negative_amount(self, tmp_path):
        bad = dict(VALID_ROW); bad["amount"] = -50.0
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1


class TestCleanAddressNormalization:
    def test_lowercases_sending_address(self, tmp_path):
        row = dict(VALID_ROW)
        row["sending_address"] = "0xAABBCCDDEE" + "A" * 30
        raw = _write_raw(tmp_path, [row])
        clean_transactions(raw, tmp_path / "clean.parquet")
        table = pq.read_table(tmp_path / "clean.parquet")
        addr = table.column("sending_address")[0].as_py()
        assert addr == addr.lower()

    def test_lowercases_receiving_address(self, tmp_path):
        row = dict(VALID_ROW)
        row["receiving_address"] = "0xBBCCDDEE11" + "B" * 30
        raw = _write_raw(tmp_path, [row])
        clean_transactions(raw, tmp_path / "clean.parquet")
        table = pq.read_table(tmp_path / "clean.parquet")
        addr = table.column("receiving_address")[0].as_py()
        assert addr == addr.lower()


class TestCleanInvalidCategoricals:
    def test_drops_invalid_transaction_type(self, tmp_path):
        bad = dict(VALID_ROW); bad["transaction_type"] = "invalid_type"
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1

    def test_drops_invalid_anomaly(self, tmp_path):
        bad = dict(VALID_ROW); bad["anomaly"] = "unknown_risk"
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1

    def test_drops_numeric_location_region(self, tmp_path):
        bad = dict(VALID_ROW); bad["location_region"] = "42"
        raw = _write_raw(tmp_path, [VALID_ROW, bad])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_cleaned"] == 1


class TestCleanEmptyInput:
    def test_empty_input_produces_empty_output(self, tmp_path):
        raw = _write_raw(tmp_path, [])
        metrics = clean_transactions(raw, tmp_path / "clean.parquet")
        assert metrics["total_raw"] == 0
        assert metrics["total_cleaned"] == 0
        assert (tmp_path / "clean.parquet").exists()

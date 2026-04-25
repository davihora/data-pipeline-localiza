"""
Unit tests for src/transforms/aggregations.py
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from transforms.aggregations import build_table1_region_risk, build_table2_top3_receivers
from tests.conftest import CLEAN_SCHEMA


def _clean_parquet(tmp_path: Path, rows: list[dict]) -> Path:
    """Write rows using CLEAN_SCHEMA (timestamp already as TIMESTAMP)."""
    if not rows:
        table = pa.table(
            {name: pa.array([], type=field.type) for name, field in zip(CLEAN_SCHEMA.names, CLEAN_SCHEMA)},
            schema=CLEAN_SCHEMA,
        )
    else:
        arrays = {
            name: pa.array([r[name] for r in rows], type=field.type)
            for name, field in zip(CLEAN_SCHEMA.names, CLEAN_SCHEMA)
        }
        table = pa.table(arrays, schema=CLEAN_SCHEMA)
    out = tmp_path / "clean.parquet"
    pq.write_table(table, out)
    return out


def _ts(year, month, day):
    return datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)


def _row(**overrides):
    base = {
        "timestamp": _ts(2023, 1, 1),
        "sending_address":   "0xaabb" + "0" * 36,
        "receiving_address": "0xccdd" + "1" * 36,
        "amount": 100.0,
        "transaction_type": "sale",
        "location_region": "North America",
        "ip_prefix": "10.0",
        "login_frequency": 1,
        "session_duration": 60,
        "purchase_pattern": "focused",
        "age_group": "new",
        "risk_score": 50.0,
        "anomaly": "low_risk",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Table 1 — region risk
# ---------------------------------------------------------------------------

class TestTable1RegionRisk:
    def test_groups_by_region(self, tmp_path):
        rows = [
            _row(location_region="North America", risk_score=60.0),
            _row(location_region="Europe",        risk_score=40.0),
            _row(location_region="Asia",          risk_score=50.0),
        ]
        src = _clean_parquet(tmp_path, rows)
        result = build_table1_region_risk(src, tmp_path / "t1.parquet")
        assert result["rows"] == 3

    def test_ordered_descending(self, tmp_path):
        rows = [
            _row(location_region="Europe",        risk_score=20.0),
            _row(location_region="North America", risk_score=80.0),
            _row(location_region="Asia",          risk_score=50.0),
        ]
        src = _clean_parquet(tmp_path, rows)
        build_table1_region_risk(src, tmp_path / "t1.parquet")
        table = pq.read_table(tmp_path / "t1.parquet")
        scores = [r.as_py() for r in table.column("avg_risk_score")]
        assert scores == sorted(scores, reverse=True)

    def test_empty_input(self, tmp_path):
        src = _clean_parquet(tmp_path, [])
        result = build_table1_region_risk(src, tmp_path / "t1.parquet")
        assert result["rows"] == 0

    def test_output_file_created(self, tmp_path):
        src = _clean_parquet(tmp_path, [_row()])
        out = tmp_path / "sub" / "t1.parquet"
        build_table1_region_risk(src, out)
        assert out.exists()

    def test_returns_dict_with_expected_keys(self, tmp_path):
        src = _clean_parquet(tmp_path, [_row()])
        result = build_table1_region_risk(src, tmp_path / "t1.parquet")
        assert "rows" in result and "output" in result


# ---------------------------------------------------------------------------
# Table 2 — top 3 receivers
# ---------------------------------------------------------------------------

class TestTable2Top3Receivers:
    def test_limits_to_3_rows(self, tmp_path):
        rows = [
            _row(receiving_address=f"0x{'aa' * 20}", amount=100.0 * i, transaction_type="sale",
                 timestamp=_ts(2023, 1, i))
            for i in range(1, 6)
        ]
        # Each row has a unique receiving_address
        for i, r in enumerate(rows):
            r["receiving_address"] = f"0x{str(i) * 40}"
        src = _clean_parquet(tmp_path, rows)
        result = build_table2_top3_receivers(src, tmp_path / "t2.parquet")
        assert result["rows"] == 3

    def test_picks_latest_sale_per_address(self, tmp_path):
        addr = "0xabcd" + "0" * 36
        rows = [
            _row(receiving_address=addr, amount=50.0,  transaction_type="sale",
                 timestamp=_ts(2023, 1, 1)),   # older, lower amount
            _row(receiving_address=addr, amount=999.0, transaction_type="sale",
                 timestamp=_ts(2023, 12, 1)),  # newer, higher amount → should be picked
        ]
        src = _clean_parquet(tmp_path, rows)
        build_table2_top3_receivers(src, tmp_path / "t2.parquet")
        table = pq.read_table(tmp_path / "t2.parquet")
        assert table.column("amount")[0].as_py() == 999.0

    def test_no_sales_returns_empty(self, tmp_path):
        rows = [
            _row(transaction_type="purchase"),
            _row(transaction_type="transfer"),
        ]
        src = _clean_parquet(tmp_path, rows)
        result = build_table2_top3_receivers(src, tmp_path / "t2.parquet")
        assert result["rows"] == 0

    def test_empty_input(self, tmp_path):
        src = _clean_parquet(tmp_path, [])
        result = build_table2_top3_receivers(src, tmp_path / "t2.parquet")
        assert result["rows"] == 0

    def test_returns_dict_with_expected_keys(self, tmp_path):
        src = _clean_parquet(tmp_path, [_row()])
        result = build_table2_top3_receivers(src, tmp_path / "t2.parquet")
        assert "rows" in result and "output" in result

    def test_ignores_non_sale_transactions(self, tmp_path):
        addr = "0xabcd" + "0" * 36
        rows = [
            _row(receiving_address=addr, amount=9999.0, transaction_type="purchase"),
            _row(receiving_address=addr, amount=1.0,    transaction_type="sale"),
        ]
        src = _clean_parquet(tmp_path, rows)
        build_table2_top3_receivers(src, tmp_path / "t2.parquet")
        table = pq.read_table(tmp_path / "t2.parquet")
        # Only the sale row should appear
        assert table.column("amount")[0].as_py() == 1.0

"""
Unit tests for src/quality/dq_suite.py — compute_custom_metrics only.
(run_gx_suite has filesystem/GX side-effects; covered by integration tests.)
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

from quality.dq_suite import DataQualityError, compute_custom_metrics
from tests.conftest import CLEAN_SCHEMA


def _write_clean(tmp_path: Path, rows: list[dict]) -> Path:
    """Write rows using CLEAN_SCHEMA."""
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


def _ts(year=2023, month=1, day=1):
    return datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)


def _row(**overrides):
    base = {
        "timestamp": _ts(),
        "sending_address":   "0x" + "a" * 40,
        "receiving_address": "0x" + "b" * 40,
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


class TestComputeCustomMetricsKeys:
    def test_returns_all_expected_keys(self, tmp_path):
        src = _write_clean(tmp_path, [_row()])
        metrics = compute_custom_metrics(src)
        expected = {
            "total_records", "null_count_per_column", "total_nulls",
            "invalid_amount_rows", "invalid_sending_addresses",
            "invalid_receiving_addresses", "risk_score_out_of_range",
            "error_count", "compliance_pct", "anomaly_breakdown",
        }
        assert expected <= set(metrics.keys())


class TestCompliancePct:
    def test_perfect_compliance(self, tmp_path):
        """A fully clean parquet should yield 100% compliance."""
        src = _write_clean(tmp_path, [_row()])
        metrics = compute_custom_metrics(src)
        assert metrics["compliance_pct"] == 100.0

    def test_invalid_address_lowers_compliance(self, tmp_path):
        """Uppercase address fails regex → error_count > 0 → compliance < 100."""
        src = _write_clean(tmp_path, [_row(sending_address="0x" + "A" * 40)])
        metrics = compute_custom_metrics(src)
        assert metrics["compliance_pct"] < 100.0


class TestNullCounts:
    def test_null_amount_counted(self, tmp_path):
        rows = [_row(), _row(amount=None)]
        src = _write_clean(tmp_path, rows)
        metrics = compute_custom_metrics(src)
        assert metrics["null_count_per_column"]["amount"] == 1

    def test_zero_nulls_on_clean_data(self, tmp_path):
        src = _write_clean(tmp_path, [_row()])
        metrics = compute_custom_metrics(src)
        assert metrics["total_nulls"] == 0


class TestInvalidAmount:
    def test_null_amount_flagged(self, tmp_path):
        src = _write_clean(tmp_path, [_row(amount=None)])
        metrics = compute_custom_metrics(src)
        assert metrics["invalid_amount_rows"] >= 1

    def test_valid_amount_not_flagged(self, tmp_path):
        src = _write_clean(tmp_path, [_row(amount=500.0)])
        metrics = compute_custom_metrics(src)
        assert metrics["invalid_amount_rows"] == 0


class TestInvalidAddresses:
    def test_uppercase_sending_flagged(self, tmp_path):
        src = _write_clean(tmp_path, [_row(sending_address="0x" + "A" * 40)])
        metrics = compute_custom_metrics(src)
        assert metrics["invalid_sending_addresses"] >= 1

    def test_lowercase_sending_clean(self, tmp_path):
        src = _write_clean(tmp_path, [_row(sending_address="0x" + "a" * 40)])
        metrics = compute_custom_metrics(src)
        assert metrics["invalid_sending_addresses"] == 0


class TestRiskScoreRange:
    def test_out_of_range_flagged(self, tmp_path):
        src = _write_clean(tmp_path, [_row(risk_score=150.0)])
        metrics = compute_custom_metrics(src)
        assert metrics["risk_score_out_of_range"] >= 1

    def test_in_range_not_flagged(self, tmp_path):
        src = _write_clean(tmp_path, [_row(risk_score=99.9)])
        metrics = compute_custom_metrics(src)
        assert metrics["risk_score_out_of_range"] == 0


class TestAnomalyBreakdown:
    def test_breakdown_keys_match_values(self, tmp_path):
        rows = [
            _row(anomaly="low_risk"),
            _row(anomaly="low_risk"),
            _row(anomaly="high_risk"),
        ]
        src = _write_clean(tmp_path, rows)
        metrics = compute_custom_metrics(src)
        assert metrics["anomaly_breakdown"]["low_risk"] == 2
        assert metrics["anomaly_breakdown"]["high_risk"] == 1

    def test_total_records(self, tmp_path):
        rows = [_row(), _row(), _row()]
        src = _write_clean(tmp_path, rows)
        metrics = compute_custom_metrics(src)
        assert metrics["total_records"] == 3


class TestDataQualityError:
    def test_is_exception(self):
        assert issubclass(DataQualityError, Exception)

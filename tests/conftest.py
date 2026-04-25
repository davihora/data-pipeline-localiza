"""
Shared pytest fixtures for all unit tests.
"""
from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Minimal valid row values
# ---------------------------------------------------------------------------
VALID_ROW = {
    "timestamp": 1672531200,          # Unix epoch int (2023-01-01)
    "sending_address": "0xaabbccddee" + "1" * 30,
    "receiving_address": "0xbbccddee11" + "2" * 30,
    "amount": 100.0,
    "transaction_type": "sale",
    "location_region": "North America",
    "ip_prefix": "192.168",
    "login_frequency": 5,
    "session_duration": 300,
    "purchase_pattern": "focused",
    "age_group": "new",
    "risk_score": 42.0,
    "anomaly": "low_risk",
}

# Matching schema for raw Parquet (types as reader produces them)
RAW_SCHEMA = pa.schema([
    ("timestamp",         pa.int64()),
    ("sending_address",   pa.string()),
    ("receiving_address", pa.string()),
    ("amount",            pa.float64()),
    ("transaction_type",  pa.string()),
    ("location_region",   pa.string()),
    ("ip_prefix",         pa.string()),
    ("login_frequency",   pa.int64()),
    ("session_duration",  pa.int64()),
    ("purchase_pattern",  pa.string()),
    ("age_group",         pa.string()),
    ("risk_score",        pa.float64()),
    ("anomaly",           pa.string()),
])

# Schema for *cleaned* Parquet (timestamp becomes pa.timestamp after DuckDB)
CLEAN_SCHEMA = pa.schema([
    ("timestamp",         pa.timestamp("us")),
    ("sending_address",   pa.string()),
    ("receiving_address", pa.string()),
    ("amount",            pa.float64()),
    ("transaction_type",  pa.string()),
    ("location_region",   pa.string()),
    ("ip_prefix",         pa.string()),
    ("login_frequency",   pa.int32()),
    ("session_duration",  pa.int32()),
    ("purchase_pattern",  pa.string()),
    ("age_group",         pa.string()),
    ("risk_score",        pa.float64()),
    ("anomaly",           pa.string()),
])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_raw_dir(tmp_path: Path) -> Path:
    """Return a tmp directory that contains one minimal valid part_001.csv."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    header = ",".join(VALID_ROW.keys())
    values = ",".join(str(v) for v in VALID_ROW.values())
    (raw_dir / "part_001.csv").write_text(f"{header}\n{values}\n")
    return raw_dir


def _make_parquet(tmp_path: Path, rows: list[dict], schema=None, filename="data.parquet") -> Path:
    """Write a list-of-dicts to a Parquet file and return its Path."""
    if not rows:
        # Build empty table with correct schema
        s = schema or RAW_SCHEMA
        table = pa.table({name: pa.array([], type=field.type) for name, field in zip(s.names, s)})
    else:
        s = schema or RAW_SCHEMA
        arrays = {}
        for name, field in zip(s.names, s):
            raw_vals = [r.get(name) for r in rows]
            arrays[name] = pa.array(raw_vals, type=field.type)
        table = pa.table(arrays, schema=s)
    out = tmp_path / filename
    pq.write_table(table, out)
    return out


@pytest.fixture()
def make_parquet(tmp_path: Path):
    """Factory fixture: make_parquet(rows, schema=None, filename='data.parquet') -> Path."""
    def _factory(rows, schema=None, filename="data.parquet"):
        return _make_parquet(tmp_path, rows, schema=schema, filename=filename)
    return _factory


@pytest.fixture()
def clean_parquet(tmp_path: Path) -> Path:
    """
    A pre-cleaned Parquet with CLEAN_SCHEMA (timestamp already as TIMESTAMP).
    Contains 3 rows across 2 regions with sale transactions.
    """
    import datetime

    rows = [
        {
            "timestamp": datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
            "sending_address":   "0xaabbccddee" + "1" * 30,
            "receiving_address": "0xbbccddee11" + "2" * 30,
            "amount": 500.0,
            "transaction_type": "sale",
            "location_region": "North America",
            "ip_prefix": "10.0",
            "login_frequency": 3,
            "session_duration": 120,
            "purchase_pattern": "focused",
            "age_group": "new",
            "risk_score": 60.0,
            "anomaly": "low_risk",
        },
        {
            "timestamp": datetime.datetime(2023, 6, 1, tzinfo=datetime.timezone.utc),
            "sending_address":   "0xaabbccddee" + "1" * 30,
            "receiving_address": "0xccddee1122" + "3" * 30,
            "amount": 200.0,
            "transaction_type": "purchase",
            "location_region": "Europe",
            "ip_prefix": "10.1",
            "login_frequency": 1,
            "session_duration": 60,
            "purchase_pattern": "random",
            "age_group": "established",
            "risk_score": 30.0,
            "anomaly": "high_risk",
        },
        {
            "timestamp": datetime.datetime(2023, 12, 1, tzinfo=datetime.timezone.utc),
            "sending_address":   "0xaabbccddee" + "1" * 30,
            "receiving_address": "0xbbccddee11" + "2" * 30,
            "amount": 800.0,
            "transaction_type": "sale",
            "location_region": "North America",
            "ip_prefix": "10.2",
            "login_frequency": 7,
            "session_duration": 300,
            "purchase_pattern": "high_value",
            "age_group": "veteran",
            "risk_score": 80.0,
            "anomaly": "moderate_risk",
        },
    ]

    arrays = {}
    for name, field in zip(CLEAN_SCHEMA.names, CLEAN_SCHEMA):
        arrays[name] = pa.array([r[name] for r in rows], type=field.type)
    table = pa.table(arrays, schema=CLEAN_SCHEMA)
    out = tmp_path / "clean.parquet"
    pq.write_table(table, out)
    return out

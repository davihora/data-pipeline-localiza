"""
Data Quality module — Great Expectations + custom metrics.

Runs two passes:
  1. GX Expectations suite on the cleaned Parquet file (generates Data Docs HTML)
  2. Custom DuckDB-based metrics: null counts, compliance %, anomaly report

Raises DataQualityError if any critical expectation fails.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import duckdb
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a critical data quality expectation fails."""


# ---------------------------------------------------------------------------
# Custom metrics (DuckDB)
# ---------------------------------------------------------------------------

def compute_custom_metrics(parquet_path: str | Path) -> dict:
    """
    Compute custom DQ metrics using DuckDB directly on the Parquet file.

    Returns a dict with:
      - total_records
      - null_count_per_column
      - error_count  (rows with any NULL in critical cols or amount <= 0)
      - compliance_pct
      - anomaly_breakdown
      - invalid_addresses_sending
      - invalid_addresses_receiving
    """
    path = str(parquet_path)
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('{path}')")

    total = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]

    # Null counts per column
    columns = [
        "timestamp", "sending_address", "receiving_address", "amount",
        "transaction_type", "location_region", "ip_prefix",
        "login_frequency", "session_duration", "purchase_pattern",
        "age_group", "risk_score", "anomaly",
    ]
    null_counts: dict[str, int] = {}
    for col in columns:
        count = con.execute(f"SELECT COUNT(*) FROM data WHERE {col} IS NULL").fetchone()[0]
        null_counts[col] = count

    total_nulls = sum(null_counts.values())

    # Invalid amount rows
    invalid_amount = con.execute(
        "SELECT COUNT(*) FROM data WHERE amount IS NULL OR amount <= 0"
    ).fetchone()[0]

    # Invalid Ethereum addresses
    invalid_sending = con.execute(
        "SELECT COUNT(*) FROM data WHERE NOT regexp_matches(sending_address, '^0x[0-9a-f]{40}$')"
    ).fetchone()[0]
    invalid_receiving = con.execute(
        "SELECT COUNT(*) FROM data WHERE NOT regexp_matches(receiving_address, '^0x[0-9a-f]{40}$')"
    ).fetchone()[0]

    error_count = total_nulls + invalid_amount + invalid_sending + invalid_receiving
    compliance_pct = round((total - error_count) / total * 100, 4) if total > 0 else 0.0

    # Anomaly breakdown
    anomaly_rows = con.execute(
        "SELECT anomaly, COUNT(*) AS count FROM data GROUP BY anomaly ORDER BY count DESC"
    ).fetchall()
    anomaly_breakdown = {row[0]: row[1] for row in anomaly_rows}

    # risk_score out of range
    risk_out_of_range = con.execute(
        "SELECT COUNT(*) FROM data WHERE risk_score < 0 OR risk_score > 100"
    ).fetchone()[0]

    con.close()

    metrics = {
        "total_records": total,
        "null_count_per_column": null_counts,
        "total_nulls": total_nulls,
        "invalid_amount_rows": invalid_amount,
        "invalid_sending_addresses": invalid_sending,
        "invalid_receiving_addresses": invalid_receiving,
        "risk_score_out_of_range": risk_out_of_range,
        "error_count": error_count,
        "compliance_pct": compliance_pct,
        "anomaly_breakdown": anomaly_breakdown,
    }
    logger.info("Custom DQ metrics: %s", json.dumps(metrics, indent=2))
    return metrics


# ---------------------------------------------------------------------------
# GX Expectations suite
# ---------------------------------------------------------------------------

def run_gx_suite(
    parquet_path: str | Path,
    gx_root: str | Path,
    raise_on_failure: bool = True,
) -> dict:
    """
    Run the Great Expectations suite against the cleaned Parquet file.

    Generates Data Docs HTML at <gx_root>/uncommitted/data_docs/local_site/.
    Returns the validation result summary.
    """
    import pandas as pd

    parquet_path = Path(parquet_path)
    gx_root = Path(gx_root)

    # Load data into pandas for GX
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    con.close()

    # ------------------------------------------------------------------
    # Build an ephemeral GX context backed by the filesystem root
    # ------------------------------------------------------------------
    context = gx.get_context(mode="file", project_root_dir=str(gx_root))

    suite_name = "transactions_suite"

    # Create or update the expectation suite
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(suite_name)

    # ------------------------------------------------------------------
    # Define expectations
    # ------------------------------------------------------------------
    ds = context.sources.add_or_update_pandas(name="transactions_ds")
    da = ds.add_dataframe_asset(name="transactions_clean")
    batch_request = da.build_batch_request(dataframe=df)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Row count
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=10_000_000)

    # Non-null critical columns
    for col in ["timestamp", "sending_address", "receiving_address",
                "amount", "transaction_type", "location_region",
                "risk_score", "anomaly"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # Categorical sets
    validator.expect_column_values_to_be_in_set(
        column="transaction_type", value_set=["sale", "purchase", "transfer"]
    )
    validator.expect_column_values_to_be_in_set(
        column="anomaly", value_set=["low_risk", "moderate_risk", "high_risk"]
    )
    validator.expect_column_values_to_be_in_set(
        column="age_group", value_set=["new", "established", "veteran"]
    )
    validator.expect_column_values_to_be_in_set(
        column="purchase_pattern", value_set=["focused", "random", "high_value"]
    )

    # Numeric ranges
    validator.expect_column_values_to_be_between(
        column="risk_score", min_value=0, max_value=100
    )
    validator.expect_column_values_to_be_between(
        column="amount", min_value=0, strict_min=True
    )
    validator.expect_column_values_to_be_between(
        column="login_frequency", min_value=0
    )
    validator.expect_column_values_to_be_between(
        column="session_duration", min_value=0
    )

    # Address regex
    validator.expect_column_values_to_match_regex(
        column="sending_address", regex=r"^0x[0-9a-f]{40}$"
    )
    validator.expect_column_values_to_match_regex(
        column="receiving_address", regex=r"^0x[0-9a-f]{40}$"
    )

    validator.save_expectation_suite(discard_failed_expectations=False)

    # ------------------------------------------------------------------
    # Run checkpoint
    # ------------------------------------------------------------------
    checkpoint = context.add_or_update_checkpoint(
        name="transactions_checkpoint",
        validator=validator,
    )
    result = checkpoint.run()

    # Build Data Docs
    context.build_data_docs()
    docs_path = gx_root / "uncommitted" / "data_docs" / "local_site" / "index.html"
    logger.info("GX Data Docs generated at: %s", docs_path)

    success = result.success
    stats = result.to_json_dict()

    if not success and raise_on_failure:
        failed_names = []
        for suite_result in result.list_validation_results():
            for r in suite_result.results:
                if not r.success:
                    failed_names.append(str(r.expectation_config.expectation_type))
        raise DataQualityError(
            f"Data quality check failed. Failed expectations: {failed_names}"
        )

    summary = {
        "success": success,
        "evaluated_expectations": stats.get("statistics", {}).get("evaluated_expectations", 0),
        "successful_expectations": stats.get("statistics", {}).get("successful_expectations", 0),
        "unsuccessful_expectations": stats.get("statistics", {}).get("unsuccessful_expectations", 0),
        "docs_path": str(docs_path),
    }
    logger.info("GX suite result: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# Main entry point (used by the Airflow task)
# ---------------------------------------------------------------------------

def run_data_quality(
    parquet_path: str | Path,
    gx_root: str | Path,
    raise_on_failure: bool = True,
) -> dict:
    """Run both custom metrics and GX expectations. Returns combined report."""
    custom = compute_custom_metrics(parquet_path)
    gx_result = run_gx_suite(parquet_path, gx_root, raise_on_failure=raise_on_failure)
    return {"custom_metrics": custom, "gx_result": gx_result}

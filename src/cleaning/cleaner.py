"""
Cleaning module — DuckDB-powered data cleansing.

Steps applied in order:
  1. Load Parquet from staging
  2. Cast timestamp (Unix epoch int → TIMESTAMP)
  3. Normalise Ethereum addresses to lowercase
  4. Drop exact duplicate rows
  5. Drop rows where any critical column is NULL
  6. Validate amount > 0 (flag negatives/zeros as invalid)
  7. Write cleaned Parquet back to staging
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# Columns that must be non-null for a row to be considered valid
CRITICAL_COLUMNS = [
    "timestamp",
    "sending_address",
    "receiving_address",
    "amount",
    "transaction_type",
    "location_region",
    "risk_score",
    "anomaly",
]

# Allowed categorical values
ALLOWED_TRANSACTION_TYPES = ("sale", "purchase", "transfer")
ALLOWED_ANOMALY_VALUES = ("low_risk", "moderate_risk", "high_risk")
ALLOWED_AGE_GROUPS = ("new", "established", "veteran")
ALLOWED_PURCHASE_PATTERNS = ("focused", "random", "high_value")

# Ethereum address regex (0x followed by exactly 40 hex chars)
ETH_ADDRESS_REGEX = r"^0x[0-9a-fA-F]{40}$"


def clean_transactions(
    input_parquet: str | Path,
    output_parquet: str | Path,
) -> dict:
    """
    Apply all cleaning steps using DuckDB and write a cleaned Parquet file.

    Returns a dict with cleaning metrics.
    """
    input_path = str(input_parquet)
    output_path = Path(output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # ------------------------------------------------------------------
    # 1. Load raw Parquet
    # ------------------------------------------------------------------
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{input_path}')")
    total_raw = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    logger.info("Loaded %d rows from %s", total_raw, input_path)

    # ------------------------------------------------------------------
    # 2. Initial cleaning CTE: cast types + normalise addresses
    # ------------------------------------------------------------------
    con.execute("""
        CREATE OR REPLACE TABLE cleaned AS
        WITH casted AS (
            SELECT
                to_timestamp(timestamp)                     AS timestamp,
                lower(trim(sending_address))                AS sending_address,
                lower(trim(receiving_address))              AS receiving_address,
                amount,
                lower(trim(transaction_type))               AS transaction_type,
                trim(location_region)                       AS location_region,
                trim(ip_prefix)                             AS ip_prefix,
                CAST(login_frequency AS INTEGER)            AS login_frequency,
                CAST(session_duration AS INTEGER)           AS session_duration,
                trim(purchase_pattern)                      AS purchase_pattern,
                trim(age_group)                             AS age_group,
                risk_score,
                lower(trim(anomaly))                        AS anomaly
            FROM raw
        ),
        -- 3. Remove exact duplicates
        deduped AS (
            SELECT DISTINCT * FROM casted
        ),
        -- 4. Drop rows with NULLs in critical columns
        no_nulls AS (
            SELECT * FROM deduped
            WHERE
                timestamp           IS NOT NULL
                AND sending_address     IS NOT NULL
                AND receiving_address   IS NOT NULL
                AND amount              IS NOT NULL
                AND transaction_type    IS NOT NULL
                AND location_region     IS NOT NULL
                AND risk_score          IS NOT NULL
                AND anomaly             IS NOT NULL
        ),
        -- 5. Drop rows with invalid amount (<= 0), invalid/numeric location_region
        --    and categorical values outside the allowed sets
        valid_data AS (
            SELECT * FROM no_nulls
            WHERE
                amount > 0
                -- location_region must not be a bare number (corrupt byte-split artefact)
                AND regexp_matches(trim(location_region), '^[A-Za-z]')
                AND transaction_type IN ('sale', 'purchase', 'transfer')
                AND anomaly          IN ('low_risk', 'moderate_risk', 'high_risk')
                AND age_group        IN ('new', 'established', 'veteran')
                AND purchase_pattern IN ('focused', 'random', 'high_value')
        )
        SELECT * FROM valid_data
    """)

    total_cleaned = con.execute("SELECT COUNT(*) FROM cleaned").fetchone()[0]

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------
    duplicates_removed = total_raw - con.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM raw)"
    ).fetchone()[0]

    null_dropped = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT DISTINCT * FROM raw)
        WHERE {" OR ".join(f"{c} IS NULL" for c in CRITICAL_COLUMNS)}
    """).fetchone()[0]

    invalid_amount = con.execute("""
        SELECT COUNT(*) FROM (SELECT DISTINCT * FROM raw)
        WHERE amount IS NOT NULL AND amount <= 0
    """).fetchone()[0]

    total_dropped = total_raw - total_cleaned

    metrics = {
        "total_raw": total_raw,
        "total_cleaned": total_cleaned,
        "total_dropped": total_dropped,
        "duplicates_removed": duplicates_removed,
        "null_critical_dropped": null_dropped,
        "invalid_amount_dropped": invalid_amount,
    }

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------
    con.execute(f"COPY cleaned TO '{output_path}' (FORMAT PARQUET, CODEC 'SNAPPY')")
    con.close()

    logger.info(
        "Cleaning complete → %s | raw=%d cleaned=%d dropped=%d",
        output_path,
        total_raw,
        total_cleaned,
        total_dropped,
    )
    return metrics

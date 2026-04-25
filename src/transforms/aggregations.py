"""
Transformations module — business aggregations via DuckDB.

Table 1:
    Regions ordered by average risk_score descending.

Table 2:
    For each receiving_address, take only the most recent "sale" transaction
    (by timestamp). From those, return the top 3 by amount, showing
    receiving_address, amount, and timestamp.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def build_table1_region_risk(
    input_parquet: str | Path,
    output_parquet: str | Path,
) -> dict:
    """
    Table 1: location_region ordered by avg(risk_score) DESC.

    Output columns: location_region, avg_risk_score, transaction_count
    """
    input_path = str(input_parquet)
    output_path = Path(output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW cleaned AS SELECT * FROM read_parquet('{input_path}')")

    con.execute("""
        CREATE OR REPLACE TABLE table1 AS
        SELECT
            location_region,
            ROUND(AVG(risk_score), 6)   AS avg_risk_score,
            COUNT(*)                     AS transaction_count
        FROM cleaned
        GROUP BY location_region
        ORDER BY avg_risk_score DESC
    """)

    result_rows = con.execute("SELECT * FROM table1").fetchall()
    con.execute(f"COPY table1 TO '{output_path}' (FORMAT PARQUET, CODEC 'SNAPPY')")
    con.close()

    logger.info(
        "Table 1 written to %s | %d region(s)", output_path, len(result_rows)
    )
    for row in result_rows:
        logger.info("  region=%-20s  avg_risk=%.4f  count=%d", *row)

    return {"rows": len(result_rows), "output": str(output_path)}


def build_table2_top3_receivers(
    input_parquet: str | Path,
    output_parquet: str | Path,
) -> dict:
    """
    Table 2: for each receiving_address, pick the most recent 'sale' transaction,
    then return the top 3 by amount.

    Output columns: receiving_address, amount, timestamp
    """
    input_path = str(input_parquet)
    output_path = Path(output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE VIEW cleaned AS SELECT * FROM read_parquet('{input_path}')")

    con.execute("""
        CREATE OR REPLACE TABLE table2 AS
        WITH latest_sale_per_receiver AS (
            SELECT
                receiving_address,
                amount,
                timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY receiving_address
                    ORDER BY timestamp DESC
                ) AS rn
            FROM cleaned
            WHERE transaction_type = 'sale'
        )
        SELECT
            receiving_address,
            amount,
            timestamp
        FROM latest_sale_per_receiver
        WHERE rn = 1
        ORDER BY amount DESC
        LIMIT 3
    """)

    result_rows = con.execute("SELECT * FROM table2").fetchall()
    con.execute(f"COPY table2 TO '{output_path}' (FORMAT PARQUET, CODEC 'SNAPPY')")
    con.close()

    logger.info(
        "Table 2 written to %s | top %d receiver(s)", output_path, len(result_rows)
    )
    for row in result_rows:
        logger.info("  address=%s  amount=%.2f  ts=%s", *row)

    return {"rows": len(result_rows), "output": str(output_path)}
